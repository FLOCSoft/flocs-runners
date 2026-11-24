import pickle
from pathlib import Path
import tempfile
from typing import TypedDict


class _ModelInfo(TypedDict):
    added_defaults: list[str]
    npix: int
    cell: float


def ensure_compatible_dr3_model(model_path: Path, output_path: Path) -> _ModelInfo:
    """Validate a DR3 DicoModel file and add missing defaults expected by DDF."""
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
    information: _ModelInfo = {
        "added_defaults": added,
        "npix": int(model["GD"]["Image"]["NPix"]),
        "cell": float(model["GD"]["Image"]["Cell"]),
    }
    staged = None
    try:
        with tempfile.NamedTemporaryFile(dir=output_path.parent, delete=False) as stream:
            staged = Path(stream.name)
            # Match the pickle protocol used by the DDF container's Python 3.9.
            pickle.dump(model, stream, protocol=4)
        staged.replace(output_path)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
    return information
