import json
import pickle
import sys


def ensure_compatible_dr3_model(model_path, output_path):
    """Validate a DR3 model and add missing defaults expected by DDF."""
    with open(model_path, "rb") as stream:
        model = pickle.load(stream, encoding="latin-1")
    if not model.get("GD") or "ModelShape" not in model or not len(model.get("Comp", {})):
        raise ValueError("Invalid DicoModel")
    added = []
    for section, values in {
        "Weight": {"EnableSigmoidTaper": 0, "SigmoidTaperInnerCutoff": 0,
                   "SigmoidTaperOuterCutoff": 0, "SigmoidTaperInnerRolloffStrength": 0.5,
                   "SigmoidTaperOuterRolloffStrength": 0.5},
        "Data": {"Dask": False},
    }.items():
        for key, value in values.items():
            if key not in model["GD"].setdefault(section, {}):
                model["GD"][section][key] = value
                added.append(section + "." + key)
    with open(output_path, "wb") as stream:
        pickle.dump(model, stream)
    return {"added_defaults": added,
            "npix": int(model["GD"]["Image"]["NPix"]), "cell": float(model["GD"]["Image"]["Cell"])}


if __name__ == "__main__":
    print("DR3_MODEL_INFO " + json.dumps(ensure_compatible_dr3_model(*sys.argv[1:])))
