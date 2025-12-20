from .utils import add_slurm_skeleton
from cyclopts import App, Parameter
from typing_extensions import Any, Annotated
import os
import structlog
import subprocess
import sys

app = App(group="Other")
logger = structlog.getLogger()


class uGMRTConfig:
    def __init__(self, msin: list[str], mode: str, scheduler: str = "singleMachine"):
        self.scheduler = scheduler
        self.mode = mode
        self.msin = msin
        try:
            from casaconfig import config

            if config.measurespath:
                self.measures_path = config.measurespath
                logger.info(f"Found CASA measures at {self.measures_path}")
        except ModuleNotFoundError:
            logger.critical("Could not determine location of CASA measures.")
            sys.exit(1)

    def run(self, workdir: str, slurm_params: dict[str, Any] = {}):
        # In case a node does not have internet access, we block CASA
        # from trying to download or update measures automatically.
        if "CASASITECONFIG" not in os.environ.keys():
            casa_site_config = os.path.join(workdir, "casasiteconfig")
            with open(casa_site_config, "w") as f:
                f.write(f"measures_path = {self.measures_path}\n")
                f.write("measures_auto_update = False\n")
                f.write("data_auto_update = False")
            os.environ["CASASITECONFIG"] = casa_site_config

        cmd = ["facetselfcal"]
        cmd += ["-i", "uGMRT_bandpass"]
        cmd += ["--noarchive"]
        cmd += ["--uvmin", 500]
        cmd += ["--uvminim", 10]
        cmd += ["--channelsout", 8]
        cmd += ["--niter", 1000]
        cmd += ["--useaoflagger"]
        cmd += ["--aoflagger-strategy", "bandpass1GMRT_StokesI.lua"]
        cmd += ["--parallelgridding", 4]
        cmd += ["--imsize", 1024]
        cmd += ["--pixelscale", 0.75]
        cmd += ["--soltype-list", "['phaseonly','complexgain']"]
        cmd += ["--solint-list", "['5sec','30m']"]
        cmd += ["--nchan-list", "[1,1]"]
        cmd += ["--soltypecycles-list", "[0,0]"]
        cmd += ["--smoothnessconstraint-list", "[0.,0.]"]
        cmd += ["--msinstartchan", 30]
        cmd += ["--robust", -0.25]
        cmd += ["--forwidefield"]
        cmd += ["--removemostlyflaggedstations"]
        cmd += ["--skymodelsetjy"]
        cmd += ["--solve-msinnchan-list", "[10,'all']"]
        cmd += ["--solve-msinstartchan-list", "[400,0]"]
        cmd += ["--useaoflaggerbeforeavg", False]
        cmd += ["--ampresetvalfactor", 100]
        cmd += ["--bandpass"]
        cmd += ["--bandpass-stop", 1]
        cmd += ["--keepusingstartingskymodel"]
        cmd += ["--useaoflagger-correcteddata"]
        cmd += ["--aoflagger-strategy-correcteddata", "bandpass2GMRT_RRLL.lua"]
        cmd += self.msin

        if self.scheduler == "slurm" and slurm_params:
            wrapped_cmd = add_slurm_skeleton(
                contents=" ".join(cmd),
                job_name=f"uGMRT_{self.mode}",
                **slurm_params,
            )
            with open("temp_jobscript.sh", "w") as f:
                f.write(wrapped_cmd)
            logger.info("Written temporary jobscript to temp_jobscript.sh")
            out = subprocess.check_output(["sbatch", "temp_jobscript.sh"]).decode(
                "utf-8"
            )
        elif self.scheduler == "singleMachine":
            logger.info(f"Running command:\n{' '.join(cmd)}")
            out = subprocess.check_output(cmd).decode("utf-8")
            print(out)


@app.command
def bandpass(
    mspath: Annotated[list[str], Parameter(help="MSes to process.")],
    rundir: Annotated[
        str,
        Parameter(help="Directory to run in."),
    ] = os.getcwd(),
    scheduler: Annotated[
        str,
        Parameter(help="System scheduler to use."),
    ] = "singleMachine",
    slurm_queue: Annotated[
        str,
        Parameter(help="Slurm queue to run jobs on."),
    ] = "",
    slurm_account: Annotated[
        str,
        Parameter(help="Slurm account to use."),
    ] = "",
    slurm_time: Annotated[
        str,
        Parameter(help="Slurm time limit to use."),
    ] = "72:00:00",
    container: Annotated[
        str,
        Parameter(help="Apptainer container to use."),
    ] = "",
):
    pass


@app.command
def target_di():
    pass


if __name__ == "__main__":
    app()
