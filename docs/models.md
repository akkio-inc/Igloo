# The model pack

Igloo runs open-weight models in its own processes, servers and workers
alike, from the **model pack**: a directory (`IGLOO_MODEL_DIR`) that an
admin installs models into with `igloo models install`. Every file of a
model is pinned by its SHA-256 and size, to an upstream revision; Igloo
checks each file as it downloads and again before a process first uses it,
and never uses one that does not match. Only models whose weights are
licensed **Apache-2.0 or MIT** are installed or loaded. Nothing is
downloaded when a query runs: a query that needs a model the pack lacks
fails, saying how to install it.

```sh
export IGLOO_MODEL_DIR=/var/lib/igloo/models
igloo models list                              # what Igloo pins, and what is installed
igloo models install chronos-bolt-small timesfm-2.5
igloo models verify                            # check every installed file again
```

Models that call providers instead, with your own keys, are reached through
[AI integrations](ai-functions.md).

## The catalog

Each version of Igloo pins these models, each to a commit of its official
Hugging Face repository:

| Name | Model | License | Size | What it does |
|---|---|---|---|---|
| `chronos-bolt-tiny`, `-mini`, `-small`, `-base` | Amazon Chronos-Bolt (9M, 21M, 48M, 205M parameters) | Apache-2.0 | 35 MB to 821 MB | Forecasts; context up to 2,048 steps; deciles |
| `chronos-2` | Amazon Chronos-2 (120M) | Apache-2.0 | 478 MB | Forecasts; context up to 8,192 steps; 21 quantiles |
| `chronos-2-small` | Amazon Chronos-2-Small (28M) | Apache-2.0 | 112 MB | As `chronos-2`, smaller; 13 quantiles |
| `timesfm-2.5` | Google TimesFM 2.5 (200M, and its 30M quantile head) | Apache-2.0 | 925 MB | Forecasts; context up to 16,384 steps; deciles |

`igloo models list` prints the catalog, each model's revision, and whether
it is installed. A model of the catalog loads only if it is the catalog's
revision, file for file: when a newer Igloo pins a newer revision, install
it again.

Models whose weights are not Apache-2.0 or MIT are not in the catalog and
cannot be installed, whatever their use elsewhere: TimesFM 3.0
(non-commercial), Moirai 2.0 (CC-BY-NC-4.0) and Timestral (a revenue-limited
license) among them. Igloo reaches TimesFM 3.0 hosted, through BigQuery's
own functions, not locally.

## Installing

```
igloo models install [--dir DIR] [--mirror URL | --from DIR] NAME...
igloo models install [--dir DIR] [--mirror URL | --from DIR] --manifest FILE
igloo models verify [--dir DIR] [NAME...]
igloo models remove [--dir DIR] NAME...
```

- `install` downloads each file over HTTPS from its pinned URL, checking
  its size and SHA-256 as it arrives, into a temporary directory in the
  pack; the model replaces an installed one of its name only once every
  file checks out. A model installed already, file for file, is left as it
  is. Interrupted, it leaves nothing behind.
- `--mirror URL` (or `IGLOO_MODEL_MIRROR`) reads `URL/<model>/<file>` in
  place of the upstream URLs, for hosts that cannot reach Hugging Face:
  copy the files there once. `--from DIR` reads `DIR/<file>` or
  `DIR/<model>/<file>`, for hosts without a network. The files are checked
  the same way.
- `verify` checks installed models' files again; `remove` removes models.
- Run them as the user Igloo runs as, or make the files readable by it.

### Models outside the catalog

`--manifest FILE` installs a model the catalog does not have, from a
manifest that pins its files. The model must be of an architecture Igloo
runs (`chronos-bolt`, `chronos-2` or `timesfm-2.5` for forecasts), and its
license must be Apache-2.0 or MIT. Its name may not be one of the
catalog's.

```json
{
  "name": "chronos-bolt-campaigns",
  "version": "2026-10-01",
  "license": "Apache-2.0",
  "kind": "forecast",
  "architecture": "chronos-bolt",
  "source": "https://models.internal/chronos-bolt-campaigns",
  "files": [
    {"path": "config.json", "url": "https://models.internal/chronos-bolt-campaigns/config.json",
     "sha256": "<64 hex digits>", "size": 1120},
    {"path": "model.safetensors", "url": "https://models.internal/chronos-bolt-campaigns/model.safetensors",
     "sha256": "<64 hex digits>", "size": 190888824}
  ]
}
```

| Key | Meaning |
|---|---|
| `name` | How SQL names the model: lower-case letters, digits, `.`, `_` and `-`, at most 64 |
| `version` | The revision the files are pinned to |
| `license` | The weights' license, an SPDX identifier: `Apache-2.0` or `MIT` |
| `kind` | `forecast` (`embedding` and `decision` are reserved for local embedding and decision models) |
| `architecture` | The code that runs it |
| `source` | Where the model comes from, for people |
| `files` | Each file's name in the model's directory, its `https` URL, SHA-256 and size in bytes |

## Where the pack lives

- **One host.** A directory on the host, or a Docker volume:
  `-v igloo-models:/var/lib/igloo/models -e IGLOO_MODEL_DIR=/var/lib/igloo/models`,
  installed with `docker run --rm -v igloo-models:/var/lib/igloo/models -e
  IGLOO_MODEL_DIR=/var/lib/igloo/models <image> models install chronos-bolt-small`.
- **Servers and workers.** Every process that may run a model needs the
  same pack, [workers](distributed-execution.md) included. Mount one shared, read-only volume at `IGLOO_MODEL_DIR` on
  every server and worker (a ReadOnlyMany volume on Kubernetes), and install
  into it from one place; or bake the pack into an image of your own that
  derives from Igloo's. Each process checks the files before it uses them,
  so a host whose copy differs refuses to run the model.
- The directory holds a directory per model, with its `manifest.json` and
  its files. Do not edit them: a changed file fails verification.

## Memory and loading

A process loads a model the first time a statement needs it: it checks the
model's files (about 2 seconds a gigabyte), maps its weights into memory
read only, and keeps it while it runs. The weights are shared by every
statement of the process; each statement needs a little memory besides, for
its series.
