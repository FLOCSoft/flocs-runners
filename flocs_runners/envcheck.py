from cyclopts import App
import os
import pathlib
import structlog

app = App(group="Other")
logger = structlog.getLogger()

essential_variables = [
    "APPTAINER_CACHEDIR",
    "APPTAINER_PULLDIR",
    "CWL_SINGULARITY_CACHE",
    "LINC_DATA_ROOT",
    "VLBI_DATA_ROOT",
]


def check_variable(var):
    if var in os.environ:
        logger.info(f"{var}: OK")
    else:
        logger.critical(f"{var}: NOT OK")


@app.command
def check():
    for var in essential_variables:
        check_variable(var)
    try:
        linc_container = pathlib.Path(os.environ["CWL_SINGULARITY_CACHE"]) / "astronrd_linc_latest.sif"
        if not (linc_container.is_symlink() or linc_container.is_file()):
            logger.critical("No suitable container found for LINC.")
    except KeyError:
        logger.critical("No suitable container found for LINC.")
    try:
        pilot_container = pathlib.Path(os.environ["CWL_SINGULARITY_CACHE"]) / "vlbi-cwl_latest.sif"
        if not (pilot_container.is_symlink() or pilot_container.is_file()):
            logger.critical("No suitable container found for PILOT.")
    except KeyError:
        logger.critical("No suitable container found for LINC.")


if __name__ == "__main__":
    app()
