# flocs-runners
Runners aiming to make running LOFAR pipelines straightforward and user-friendly.

## Installation
The runners are simply installed via pip:

```bash
pip install git+https://github.com/FLOCSoft/flocs-runners.git
```

it is recommended to use a virtual environment for this.

Some environment setup is required for everything to operate correctly. Please follow the instructions on the [FLOCS homepage](https://tikk3r.github.io/flocs/docs/using-flocs.html#using-containerstoilslurmall-of-the-above).

## DDF pipeline

`flocs-run ddf-pipeline` enables `--lite` by default. It uses an explicitly configured input model, or tries to obtain a usable LoTSS-DR3 model for the field. If no usable DR3 model is available, it warns and falls back to a full run.

DR3 models are validated without rewriting them. Missing required DDF settings or incompatible image geometry cause a warning and a full-run fallback.

Use `--no-lite` to request a full run without looking up a DR3 model or issuing a fallback warning. This option conflicts with a configured `[inputmodel] basedicomodel`; remove that setting or use `--lite`.

Restarts reuse their saved configuration without fetching a new model or changing mode. `--no-lite` also rejects a saved configuration containing an input model.
