// Package models carries out Snowflake's ML classes and Igloo's own on
// Igloo's engine (package ml): training and the methods of FORECAST,
// ANOMALY_DETECTION, CLASSIFICATION and TOP_INSIGHTS models, of the
// IGLOO.ML classes that bring BigQuery ML's model types (igloo.go),
// Databricks' train-and-score functions, the store of models' rows in the
// metadata database, and their blobs in the warehouse's object storage.
// Package pgwire reads the statements, checks privileges and reads
// training data as the user; see docs/ml-functions.md.
package models

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/ml"
)

// The classes, by their names after SNOWFLAKE.ML.
const (
	ClassForecast       = "FORECAST"
	ClassAnomaly        = "ANOMALY_DETECTION"
	ClassClassification = "CLASSIFICATION"
	ClassTopInsights    = "TOP_INSIGHTS"
	// ClassValue is a regression ai_predict_value trains, never stored.
	ClassValue = "VALUE"
)

// format is the version of the blob's layout.
const format = 1

// Artifact is what a model's blob holds: everything its methods need.
type Artifact struct {
	Format     int         `json:"format"`
	Class      string      `json:"class"`
	Classifier *Classifier `json:"classifier,omitempty"`
	Forecaster *Forecaster `json:"forecaster,omitempty"`
	Regressor  *Regressor  `json:"regressor,omitempty"`
	// Igloo's classes' models.
	Supervised  *Supervised  `json:"supervised,omitempty"`
	Clusters    *Clusters    `json:"clusters,omitempty"`
	Components  *Components  `json:"components,omitempty"`
	Factors     *Factors     `json:"factors,omitempty"`
	Autoencoder *Autoencoder `json:"autoencoder,omitempty"`
}

// iglooModel is a model of one of Igloo's classes.
type iglooModel interface {
	rowModel
	prepare()
	// method computes a table method; evaluateOn EVALUATE on given rows.
	method(class, method string) (*Result, error)
	evaluateOn(t *Table) (*Evaluation, error)
	// summary sets what a model's row records of it.
	summary(out map[string]any)
}

// igloo returns the model of one of Igloo's classes the artifact holds,
// or nil.
func (a *Artifact) igloo() iglooModel {
	switch {
	case a.Supervised != nil:
		return a.Supervised
	case a.Clusters != nil:
		return a.Clusters
	case a.Components != nil:
		return a.Components
	case a.Factors != nil:
		return a.Factors
	case a.Autoencoder != nil:
		return a.Autoencoder
	}
	return nil
}

// Encode returns the artifact's blob and its SHA-256.
func (a *Artifact) Encode() ([]byte, string, error) {
	a.Format = format
	return ml.Encode(a)
}

// DecodeArtifact reads a blob, checking it against sha.
func DecodeArtifact(blob []byte, sha string) (*Artifact, error) {
	var a Artifact
	if err := ml.Decode(blob, sha, &a); err != nil {
		return nil, err
	}
	if a.Format != format {
		return nil, fmt.Errorf("model blob %s has format %d; this version of Igloo reads %d (create the model again)", sha, a.Format, format)
	}
	a.prepare()
	return &a, nil
}

func (a *Artifact) prepare() {
	if a.Classifier != nil {
		a.Classifier.Model.Prepare()
	}
	if a.Forecaster != nil {
		a.Forecaster.prepare()
	}
	if a.Regressor != nil {
		a.Regressor.Model.Prepare()
	}
	if m := a.igloo(); m != nil {
		m.prepare()
	}
}

// Prepare readies a model just trained for scoring, as decoding does.
func (a *Artifact) Prepare() { a.prepare() }

// Feature is a model's input column, and how it was read in training,
// which scoring reads it as again.
type Feature struct {
	Name string `json:"name"`
	// Type is number, boolean, string or timestamp.
	Type string `json:"type"`
}

// Features returns what a scorer needs of the artifact's features.
func (a *Artifact) Features() []Feature {
	switch {
	case a.Classifier != nil:
		return a.Classifier.Features
	case a.Regressor != nil:
		return a.Regressor.Features
	}
	return nil
}

// errBadInput is wrapped by errors in what users give a model.
var errBadInput = errors.New("invalid input")

// InputError reports whether err is about what the user gave, rather
// than Igloo's failure.
func InputError(err error) bool { return errors.Is(err, errBadInput) }

func inputErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errBadInput, fmt.Sprintf(format, args...))
}

// featureFrame reads the table's feature columns into a frame, in the
// order names lists them.
func featureFrame(t *Table, names []string) (*ml.Frame, []Feature, error) {
	f := &ml.Frame{N: t.N}
	var feats []Feature
	for _, name := range names {
		c := t.Col(name)
		if c == nil {
			return nil, nil, inputErr("no column %s", name)
		}
		col, err := c.Feature()
		if err != nil {
			return nil, nil, inputErr("%v", err)
		}
		f.Cols = append(f.Cols, col)
		feats = append(feats, Feature{Name: c.Name, Type: c.Type.String()})
	}
	return f, feats, nil
}

// labelText is how a class label shows: booleans as Snowflake shows them
// (True, False), numbers shortest.
func labelText(c *TableCol, i int) string {
	switch c.Type {
	case TypeBool:
		if c.Str[i] == "true" {
			return "True"
		}
		return "False"
	case TypeNumber:
		return strconv.FormatFloat(c.Num[i], 'f', -1, 64)
	case TypeTime:
		return c.Time(i).Format("2006-01-02 15:04:05.999999")
	}
	return c.Str[i]
}

// otherNames lists the table's columns but those named.
func otherNames(t *Table, except ...string) []string {
	var out []string
	for _, c := range t.Cols {
		skip := false
		for _, e := range except {
			if strings.EqualFold(c.Name, e) {
				skip = true
			}
		}
		if !skip {
			out = append(out, c.Name)
		}
	}
	return out
}
