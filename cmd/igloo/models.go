package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

const modelsUsage = `usage:
  igloo models list [--dir DIR]                      the models Igloo pins, and those installed
  igloo models install [--dir DIR] [--mirror URL | --from DIR] NAME...
  igloo models install [--dir DIR] [--mirror URL | --from DIR] --manifest FILE
  igloo models verify [--dir DIR] [NAME...]          check installed models' files
  igloo models remove [--dir DIR] NAME...

DIR defaults to IGLOO_MODEL_DIR, and the mirror to IGLOO_MODEL_MIRROR (docs/models.md).`

// modelsCommand runs `igloo models ...`, the admin's step that installs the
// model pack: nothing else downloads models.
func modelsCommand(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New(modelsUsage)
	}
	fs := flag.NewFlagSet("igloo models "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("dir", os.Getenv("IGLOO_MODEL_DIR"), "the model pack's directory")
	mirror := fs.String("mirror", os.Getenv("IGLOO_MODEL_MIRROR"), "a mirror holding <model>/<file>")
	from := fs.String("from", "", "a local directory holding the files")
	manifest := fs.String("manifest", "", "a manifest of a model outside the catalog")
	if err := fs.Parse(args[1:]); err != nil {
		return fmt.Errorf("%v\n%s", err, modelsUsage)
	}
	names := fs.Args()
	needDir := func() error {
		if *dir == "" {
			return errors.New("the model pack's directory is not set: set IGLOO_MODEL_DIR or pass --dir")
		}
		if !filepath.IsAbs(*dir) {
			return fmt.Errorf("--dir must be an absolute path, got %q", *dir)
		}
		return nil
	}
	switch args[0] {
	case "list":
		return listModels(*dir, stdout)
	case "install":
		if err := needDir(); err != nil {
			return err
		}
		var todo []modelpack.Manifest
		if *manifest != "" {
			if len(names) > 0 {
				return errors.New("give --manifest or model names, not both")
			}
			m, err := modelpack.ReadManifest(*manifest)
			if err != nil {
				return err
			}
			todo = append(todo, m)
		}
		for _, n := range names {
			m, ok := modelpack.Lookup(n)
			if !ok {
				return fmt.Errorf("Igloo pins no model %q; `igloo models list` lists those it does, and --manifest installs another", n)
			}
			todo = append(todo, m)
		}
		if len(todo) == 0 {
			return errors.New(modelsUsage)
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		src := modelpack.Source{Mirror: *mirror, From: *from, Progress: func(model, file string, size int64, done bool) {
			if !done {
				fmt.Fprintf(stdout, "%s: %s (%s)\n", model, file, bytesText(size))
			}
		}}
		for _, m := range todo {
			installed, err := modelpack.Install(ctx, *dir, m, src)
			if err != nil {
				return err
			}
			if installed {
				fmt.Fprintf(stdout, "%s: installed %s, %s, verified\n", m.Name, short(m.Version), m.License)
			} else {
				fmt.Fprintf(stdout, "%s: installed already\n", m.Name)
			}
		}
		return nil
	case "verify":
		if err := needDir(); err != nil {
			return err
		}
		p := modelpack.Open(*dir)
		if len(names) == 0 {
			installed, bad, err := p.Installed()
			if err != nil {
				return err
			}
			for _, m := range installed {
				names = append(names, m.Name)
			}
			for n, err := range bad {
				fmt.Fprintf(stdout, "%s: %v\n", n, err)
			}
			if len(bad) > 0 {
				defer func() { fmt.Fprintln(stdout, "some models' manifests cannot be read") }()
			}
		}
		var failed []string
		for _, n := range names {
			if err := p.Verify(n); err != nil {
				fmt.Fprintf(stdout, "%s: %v\n", n, err)
				failed = append(failed, n)
				continue
			}
			fmt.Fprintf(stdout, "%s: verified\n", n)
		}
		if len(failed) > 0 {
			return fmt.Errorf("%s failed verification", strings.Join(failed, ", "))
		}
		return nil
	case "remove":
		if err := needDir(); err != nil {
			return err
		}
		if len(names) == 0 {
			return errors.New(modelsUsage)
		}
		for _, n := range names {
			if err := modelpack.Remove(*dir, n); err != nil {
				return err
			}
			fmt.Fprintf(stdout, "%s: removed\n", n)
		}
		return nil
	}
	return errors.New(modelsUsage)
}

func listModels(dir string, stdout io.Writer) error {
	installed := map[string]modelpack.Manifest{}
	if dir != "" {
		ms, bad, err := modelpack.Open(dir).Installed()
		if err != nil {
			return err
		}
		for _, m := range ms {
			installed[m.Name] = m
		}
		for n, err := range bad {
			fmt.Fprintf(stdout, "%s: %v\n", n, err)
		}
	}
	w := tabwriter.NewWriter(stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tKIND\tLICENSE\tSIZE\tREVISION\tINSTALLED")
	catalog := modelpack.Catalog()
	for _, m := range catalog {
		state := "no"
		if i, ok := installed[m.Name]; ok {
			state = "yes"
			if i.Version != m.Version {
				state = "another revision: install again"
			}
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", m.Name, m.Kind, m.License, bytesText(m.Bytes()), short(m.Version), state)
	}
	var others []string
	for n := range installed {
		if _, ok := modelpack.Lookup(n); !ok {
			others = append(others, n)
		}
	}
	slices.Sort(others)
	for _, n := range others {
		m := installed[n]
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\tyes (from a manifest)\n", m.Name, m.Kind, m.License, bytesText(m.Bytes()), short(m.Version))
	}
	if dir == "" {
		defer fmt.Fprintln(stdout, "IGLOO_MODEL_DIR is not set: no model is installed")
	}
	return w.Flush()
}

func bytesText(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%d KB", (n+1023)/1024)
}

func short(v string) string {
	if len(v) > 12 {
		return v[:12]
	}
	return v
}
