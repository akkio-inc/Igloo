package ai

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Providers an integration can reach.
const (
	// ProviderOpenAI is OpenAI's API and every endpoint that speaks it:
	// vLLM, llama.cpp's server, Ollama, OpenRouter. Its decision models
	// answer at /decisions, in OpenAI's shape.
	ProviderOpenAI = "openai"
	// ProviderAzure is Azure OpenAI's v1 API, with its api-key header.
	ProviderAzure = "azure_openai"
	// ProviderAnthropic is Anthropic's Messages API.
	ProviderAnthropic = "anthropic"
	// ProviderGemini is Google's Gemini API, with an API key.
	ProviderGemini = "gemini"
	// ProviderVertex is Gemini on Vertex AI, with a service account's key
	// or an express-mode API key.
	ProviderVertex = "vertex"
	// ProviderBedrock is Amazon Bedrock's Converse API, signed with AWS
	// credentials or a Bedrock API key.
	ProviderBedrock = "bedrock"
	// ProviderDecisions is a decision endpoint that takes a state and named
	// questions (Liquid, Portkey's gateway, servers of open decision
	// models); it serves AI_FILTER, AI_CLASSIFY and AI_DECIDE alone.
	ProviderDecisions = "decisions"
	// ProviderNixtla is Nixtla's TimeGPT API, or a deployment of it that
	// speaks it (Microsoft Foundry's): time series models alone.
	ProviderNixtla = "nixtla"
	// ProviderSageMaker is SageMaker endpoints of Chronos models
	// (InvokeEndpoint), signed with AWS credentials: time series models
	// alone.
	ProviderSageMaker = "sagemaker"
	// ProviderBigQuery is BigQuery's AI.FORECAST, run as a query in the
	// customer's Google Cloud project with a service account's key: time
	// series models alone, TimesFM 3.0 among them. The series go to
	// BigQuery.
	ProviderBigQuery = "bigquery"
)

// Providers lists the providers.
var Providers = []string{ProviderOpenAI, ProviderAzure, ProviderAnthropic, ProviderGemini, ProviderVertex, ProviderBedrock,
	ProviderDecisions, ProviderNixtla, ProviderSageMaker, ProviderBigQuery}

// defaultBase is each provider's own address, where BASE_URL is not given.
var defaultBase = map[string]string{
	ProviderOpenAI:    "https://api.openai.com/v1",
	ProviderAnthropic: "https://api.anthropic.com",
	ProviderGemini:    "https://generativelanguage.googleapis.com/v1beta",
	ProviderNixtla:    "https://api.nixtla.io",
	ProviderBigQuery:  "https://bigquery.googleapis.com/bigquery/v2",
}

// Price is what a model costs, in US dollars per million tokens.
type Price struct {
	Input, Output float64
}

// Integration is how Igloo reaches one provider with a customer's keys, and
// which of its models SQL may use. Admins create integrations; users need
// USAGE on one to call it.
type Integration struct {
	// Name is a lowercase SQL name, unique among all integrations,
	// notification integrations included.
	Name     string
	Provider string
	// BaseURL is where the provider's API is, such as
	// https://api.openai.com/v1; empty means the provider's own.
	BaseURL string
	// APIKey, Headers, the AWS credentials and ServiceAccount are stored
	// encrypted and never shown.
	APIKey string
	// Headers are sent with every request, as a gateway's own key.
	Headers map[string]string
	// AWSAccessKeyID, AWSSecretAccessKey and AWSSessionToken sign Bedrock's
	// requests.
	AWSAccessKeyID, AWSSecretAccessKey, AWSSessionToken string
	// ServiceAccount is a Google service account's JSON key, for Vertex AI.
	ServiceAccount string
	// APIVersion is Anthropic's API version, or Azure's api-version.
	APIVersion string
	// Region is Bedrock's AWS region, or Vertex AI's location; Project is
	// Vertex AI's Google Cloud project.
	Region, Project string
	// Models maps the model names SQL uses, Snowflake's among them, to the
	// provider's: AI_COMPLETE('claude-4-sonnet', ...) calls
	// Models["claude-4-sonnet"]. A name not in it fails; nothing falls back.
	Models map[string]string
	// DefaultModel serves the functions whose model Snowflake chooses
	// (AI_CLASSIFY, AI_FILTER, AI_SENTIMENT, AI_EXTRACT, AI_SUMMARIZE,
	// AI_TRANSLATE, AI_REDACT, AI_AGG); EmbeddingModel AI_SIMILARITY
	// without a model; DecisionModel, when set, answers AI_FILTER,
	// AI_CLASSIFY and AI_DECIDE as a decision model does, with
	// probabilities. Each is a name in Models or the provider's own.
	DefaultModel, EmbeddingModel, DecisionModel string
	// Prices are by the provider's model name.
	Prices map[string]Price
	// MaxConcurrency bounds the requests in flight to the provider from
	// this replica; 0 means defaultConcurrency.
	MaxConcurrency int
	Enabled        bool
	Comment        string
	CreatedBy      string
	CreatedAt      time.Time
}

const (
	defaultConcurrency = 8
	maxConcurrency     = 256
)

// concurrency is how many requests may be in flight to the provider.
func (in *Integration) concurrency() int {
	if in.MaxConcurrency > 0 {
		return in.MaxConcurrency
	}
	return defaultConcurrency
}

// Base is the provider's API address, without a trailing slash.
func (in *Integration) Base() string {
	if in.BaseURL != "" {
		return strings.TrimRight(in.BaseURL, "/")
	}
	switch in.Provider {
	case ProviderBedrock:
		return "https://bedrock-runtime." + in.Region + ".amazonaws.com"
	case ProviderSageMaker:
		return "https://runtime.sagemaker." + in.Region + ".amazonaws.com"
	case ProviderVertex:
		if in.Project == "" {
			// Express mode, with an API key.
			return "https://aiplatform.googleapis.com/v1/publishers/google/models"
		}
		host := in.Region + "-aiplatform.googleapis.com"
		if in.Region == "global" {
			host = "aiplatform.googleapis.com"
		}
		return "https://" + host + "/v1/projects/" + in.Project + "/locations/" + in.Region + "/publishers/google/models"
	}
	return defaultBase[in.Provider]
}

// Target is where the integration sends, without secrets: the scheme and
// host of its API.
func (in *Integration) Target() string {
	u, err := url.Parse(in.Base())
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// Decides reports whether the provider has decision models.
func (in *Integration) Decides() bool {
	switch in.Provider {
	case ProviderOpenAI, ProviderAzure, ProviderDecisions:
		return true
	}
	return false
}

// Model returns the provider's model for name, as SQL names it, and
// whether the integration maps it. Names compare without regard to case.
func (in *Integration) Model(name string) (string, bool) {
	if m, ok := in.Models[strings.ToLower(strings.TrimSpace(name))]; ok {
		return m, true
	}
	return "", false
}

// resolve is a model setting (DefaultModel and the like): a name Models
// maps, or the provider's own.
func (in *Integration) resolve(name string) string {
	if m, ok := in.Model(name); ok {
		return m
	}
	return name
}

// Price returns model's price, and whether the integration has one.
func (in *Integration) Price(model string) (Price, bool) {
	p, ok := in.Prices[model]
	return p, ok
}

// secrets lists what of in must never be shown.
func (in *Integration) secrets() []string {
	out := []string{in.APIKey, in.AWSSecretAccessKey, in.AWSSessionToken, in.ServiceAccount}
	for _, v := range in.Headers {
		out = append(out, v)
		out = append(out, strings.FieldsFunc(v, func(r rune) bool { return strings.ContainsRune(" \t,;:=", r) })...)
	}
	if in.ServiceAccount != "" {
		var sa struct {
			PrivateKey   string `json:"private_key"`
			PrivateKeyID string `json:"private_key_id"`
		}
		if json.Unmarshal([]byte(in.ServiceAccount), &sa) == nil {
			out = append(out, sa.PrivateKey, sa.PrivateKeyID)
		}
	}
	return out
}

// Scrub replaces the integration's secrets in s with ***.
func (in *Integration) Scrub(s string) string {
	secrets := slices.DeleteFunc(in.secrets(), func(x string) bool { return len(x) < 6 })
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	for _, x := range secrets {
		s = strings.ReplaceAll(s, x, "***")
	}
	return s
}

// Check validates the integration's settings; checkURL vets its address
// (Guard.CheckURL), and may be nil.
func (in *Integration) Check(checkURL func(string) error) error {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
	}
	if !slices.Contains(Providers, in.Provider) {
		return invalid("unknown PROVIDER %s; use one of %s", quote(in.Provider), strings.Join(Providers, ", "))
	}
	switch in.Provider {
	case ProviderAzure, ProviderDecisions:
		if in.BaseURL == "" {
			return invalid("a %s integration needs BASE_URL", in.Provider)
		}
	case ProviderSageMaker:
		if in.Region == "" && in.BaseURL == "" {
			return invalid("a sagemaker integration needs REGION")
		}
		if in.AWSAccessKeyID == "" || in.AWSSecretAccessKey == "" {
			return invalid("a sagemaker integration needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
		}
	case ProviderBigQuery:
		if in.ServiceAccount == "" || in.Project == "" {
			return invalid("a bigquery integration needs a SERVICE_ACCOUNT key and PROJECT, the project its queries run and are billed in")
		}
		if err := checkServiceAccount(in.ServiceAccount, checkURL); err != nil {
			return err
		}
	case ProviderBedrock:
		if in.Region == "" && in.BaseURL == "" {
			return invalid("a bedrock integration needs REGION")
		}
		if in.APIKey == "" && (in.AWSAccessKeyID == "" || in.AWSSecretAccessKey == "") {
			return invalid("a bedrock integration needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or a Bedrock API_KEY")
		}
	case ProviderVertex:
		switch {
		case in.ServiceAccount != "" && (in.Project == "" || in.Region == ""):
			return invalid("a vertex integration with a SERVICE_ACCOUNT needs PROJECT and REGION")
		case in.ServiceAccount == "" && in.APIKey == "":
			return invalid("a vertex integration needs a SERVICE_ACCOUNT key or an express-mode API_KEY")
		}
		if in.ServiceAccount != "" {
			if err := checkServiceAccount(in.ServiceAccount, checkURL); err != nil {
				return err
			}
		}
	}
	if !slices.Contains([]string{ProviderBedrock, ProviderVertex, ProviderDecisions, ProviderSageMaker, ProviderBigQuery}, in.Provider) &&
		in.APIKey == "" && len(in.Headers) == 0 && in.BaseURL == "" {
		return invalid("a %s integration needs an API_KEY", in.Provider)
	}
	if (in.Provider == ProviderBedrock || in.Provider == ProviderVertex || in.Provider == ProviderSageMaker) && in.Region != "" &&
		!validRegion(in.Region) {
		return invalid("REGION %s is not a region's name", quote(in.Region))
	}
	if in.Provider == ProviderBigQuery && in.Region != "" && !validRegion(strings.ToLower(in.Region)) {
		return invalid("REGION %s is not a BigQuery location, such as 'US', 'EU' or 'us-central1'", quote(in.Region))
	}
	if in.Project != "" && !validRegion(in.Project) {
		return invalid("PROJECT %s is not a Google Cloud project's ID", quote(in.Project))
	}
	if checkURL != nil {
		if err := checkURL(in.Base()); err != nil {
			return err
		}
	}
	for k, v := range in.Headers {
		if !validHeaderName(k) || strings.ContainsAny(v, "\r\n") {
			return invalid("HEADERS has an invalid header %s", quote(k))
		}
		switch strings.ToLower(k) {
		case "host", "content-length", "content-type", "transfer-encoding", "connection":
			return invalid("HEADERS cannot set %s", k)
		}
	}
	for alias, model := range in.Models {
		if alias == "" || strings.TrimSpace(model) == "" {
			return invalid("MODELS maps names to the provider's models, such as ('claude-4-sonnet' = 'claude-sonnet-4-5')")
		}
	}
	if in.Provider == ProviderDecisions {
		if in.DecisionModel == "" {
			return invalid("a decisions integration needs DECISION_MODEL")
		}
		if in.DefaultModel != "" || in.EmbeddingModel != "" {
			return invalid("a decisions integration serves decision models alone; set DEFAULT_MODEL and EMBEDDING_MODEL on another integration")
		}
	}
	if in.DecisionModel != "" && !in.Decides() {
		return invalid("%s has no decision models; DECISION_MODEL needs an openai, azure_openai or decisions integration", in.Provider)
	}
	if in.EmbeddingModel != "" && in.Provider == ProviderAnthropic {
		return invalid("Anthropic has no embedding models; set EMBEDDING_MODEL on another integration")
	}
	for model, p := range in.Prices {
		if p.Input < 0 || p.Output < 0 {
			return invalid("PRICES for %s must not be negative", quote(model))
		}
	}
	if in.MaxConcurrency < 0 || in.MaxConcurrency > maxConcurrency {
		return invalid("MAX_CONCURRENCY must be from 1 to %d", maxConcurrency)
	}
	return nil
}

// checkServiceAccount checks a Google service account's JSON key, and its
// token URI with checkURL, if set.
func checkServiceAccount(key string, checkURL func(string) error) error {
	var raw map[string]any
	if err := json.Unmarshal([]byte(key), &raw); err != nil {
		return fmt.Errorf("%w: SERVICE_ACCOUNT must be a service account's JSON key", ErrInvalid)
	}
	typ, _ := raw["type"].(string)
	email, _ := raw["client_email"].(string)
	pk, _ := raw["private_key"].(string)
	if typ != "service_account" || email == "" || pk == "" {
		return fmt.Errorf("%w: SERVICE_ACCOUNT must be a service account's JSON key, with client_email and private_key", ErrInvalid)
	}
	if uri, _ := raw["token_uri"].(string); uri != "" && checkURL != nil {
		if err := checkURL(uri); err != nil {
			return err
		}
	}
	return nil
}

func validRegion(s string) bool {
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return s != ""
}

func validHeaderName(k string) bool {
	if k == "" {
		return false
	}
	for _, r := range k {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", r)) {
			return false
		}
	}
	return true
}

// Clone returns a copy of in that shares no maps with it.
func (in Integration) Clone() Integration {
	in.Headers = maps.Clone(in.Headers)
	in.Models = maps.Clone(in.Models)
	in.Prices = maps.Clone(in.Prices)
	return in
}

// Grant lets a user, or the holders of a role, call an integration: GRANT
// USAGE ON INTEGRATION.
type Grant struct {
	Integration string
	Grantee     string
	// GranteeType is auth.GranteeUser or auth.GranteeRole.
	GranteeType string
	GrantedBy   string
	GrantedAt   time.Time
}
