from .utils import (
    add_slurm_skeleton_ddf,
    detect_compute_cluster,
    extract_obsid_from_ms,
    get_container_env_var,
)
import glob
import os
import structlog
import shutil
import subprocess
import tempfile
from time import gmtime, strftime
from cyclopts import Parameter
from typing_extensions import Annotated, Optional

logger = structlog.getLogger()


class DDFConfig:
    """"""

    def __init__(
        self,
        mspath: str,
        ddfconfig: str,
        outdir: str = os.getcwd(),
    ):
        self.outdir = outdir
        self.cluster = detect_compute_cluster()
        self.mspath = mspath
        self.ddfconfig = ddfconfig

        filedir = os.path.join(mspath, "*pre-cal.ms")
        logger.info(f"Searching {filedir}")
        files = sorted(glob.glob(filedir))
        logger.info(f"Found {len(files)} files")

        try:
            self.obsid = extract_obsid_from_ms(files[0])
        except IndexError:
            self.obsid = "unknown"

    def setup_rundir(self, workdir):
        self.rundir = tempfile.mkdtemp(prefix=f"tmp.DDF-pipeline_L{self.obsid}.", dir=workdir)

    def move_results_from_rundir(self):
        date = strftime("%Y_%m_%d-%H_%M_%S", gmtime())
        outpath = os.path.join(self.outdir, f"DDF-pipeline_L{self.obsid}_{date}")
        logger.info(f"Copying results to: {outpath}")
        shutil.move(self.rundir, outpath)

    def run_workflow(
        self,
        scheduler: str = "slurm",
        workdir: str = os.getcwd(),
        slurm_params: dict = {},
        restart: bool = False,
        use_node_scratch: bool = False,
    ):
        if not restart and not use_node_scratch:
            self.setup_rundir(workdir)
            self.restarting = False
        else:
            self.rundir = workdir
            self.restarting = True
            logger.info(f"Attempting to restart existing run from {self.rundir}.")
        self.setup_apptainer_variables(self.rundir)
        ddf_container = self.verify_ddf_container()
        logger.info(f"Running DDF-pipeline under {scheduler} in {self.rundir}")

        if scheduler == "slurm":
            wrapped_cmd = add_slurm_skeleton_ddf(
                data_dir=self.mspath,
                workdir=workdir,
                configfile=self.ddfconfig,
                job_name=f"DDF-pipeline_{self.obsid}",
                cluster=self.cluster,
                **slurm_params,
            )
            with open("temp_jobscript.sh", "w") as f:
                f.write(wrapped_cmd)
            logger.info("Written temporary jobscript to temp_jobscript.sh")
            out = subprocess.check_output(["sbatch", "temp_jobscript.sh"]).decode("utf-8")
            print(out)
        elif scheduler == "singleMachine":
            cmd = f"apptainer exec {ddf_container} make_mslists.py"
            logger.info(f"Running command:\n{cmd}")
            out = subprocess.check_output(cmd.split(" "))

            cmd = f"apptainer exec {ddf_container} pipeline.py {self.ddfconfig}"
            logger.info(f"Running command:\n{cmd}")
            try:
                out = subprocess.check_output(cmd.split(" "))
                with open("log_DDF-pipeline.txt", "wb") as f:
                    f.write(out)
                self.move_results_from_rundir()
            except subprocess.CalledProcessError as e:
                with open("log_DDF-pipeline.txt", "wb") as f:
                    f.write(e.stdout)
                if e.stderr:
                    with open("log_DDF-pipeline_err.txt", "wb") as f:
                        f.write(e.stderr)

    def verify_ddf_container(self) -> str:
        ddf_container = os.path.join(os.environ["CWL_SINGULARITY_CACHE"], "ddf-pipeline.sif")
        if not os.path.isfile(ddf_container):
            logger.critical("Container `ddf-pipeline.sif` not found in $CWL_SINGULARITY_CACHE")
            raise RuntimeError("No viable ddf-pipeline container was found.")
        return ddf_container

    def setup_apptainer_variables(self, workdir):
        try:
            out = subprocess.check_output(["singularity", "--version"]).decode("utf-8").strip()
        except subprocess.CalledProcessError:
            out = subprocess.check_output(["apptainer", "--version"]).decode("utf-8").strip()
        if "apptainer" in out:
            os.environ["APPTAINERENV_LINC_DATA_ROOT"] = os.environ["LINC_DATA_ROOT"]
            os.environ["APPTAINERENV_PREPEND_PATH"] = f"{os.environ['LINC_DATA_ROOT']}/scripts"
            os.environ["APPTAINERENV_PYTHONPATH"] = f"{os.environ['LINC_DATA_ROOT']}/scripts:$PYTHONPATH"
            os.environ["PATH"] = os.environ["APPTAINERENV_PREPEND_PATH"] + ":" + os.environ["PATH"]
            if "APPTAINER_BINDPATH" not in os.environ:
                os.environ["APPTAINER_BINDPATH"] = (
                    f"{os.environ['LINC_DATA_ROOT']}:/opt/lofar/LINC"
                    + f",{os.environ['LINC_DATA_ROOT']}:/opt/lofar/VLBI-cwl"  # VLBI-cwl is earlier in PATH, this is intentional.
                    + f",{os.path.dirname(workdir)}"
                )
            else:
                os.environ["APPTAINER_BINDPATH"] = (
                    f"{os.environ['LINC_DATA_ROOT']}:/opt/lofar/LINC"
                    + f",{os.environ['LINC_DATA_ROOT']}:/opt/lofar/VLBI-cwl"  # VLBI-cwl is earlier in PATH, this is intentional.
                    + f",{workdir}"
                    + f",{os.environ['APPTAINER_BINDPATH']}"
                )
        elif "singularity" in out:
            os.environ["SINGULARITYENV_LINC_DATA_ROOT"] = os.environ["LINC_DATA_ROOT"]
            os.environ["SINGULARITYENV_PREPEND_PATH"] = f"{os.environ['LINC_DATA_ROOT']}/scripts"
            # Note that cwltool for some reason does not inherit this.
            os.environ["SINGULARITYENV_PYTHONPATH"] = f"{os.environ['LINC_DATA_ROOT']}/scripts:$PYTHONPATH"
            os.environ["PATH"] = os.environ["SINGULARITYENV_PREPEND_PATH"] + ":" + os.environ["PATH"]
            if "SINGULARITY_BINDPATH" not in os.environ:
                os.environ["SINGULARITY_BINDPATH"] = (
                    f"{os.path.dirname(os.environ['LINC_DATA_ROOT'])}"
                    + f",{os.path.dirname(os.environ['VLBI_DATA_ROOT'])}"
                    + f",{os.path.dirname(workdir)}"
                )
            else:
                os.environ["SINGULARITY_BINDPATH"] = (
                    f"{os.path.dirname(os.environ['LINC_DATA_ROOT'])}"
                    + f",{os.path.dirname(os.environ['VLBI_DATA_ROOT'])}"
                    + f",{os.path.dirname(workdir)}"
                    + f",{os.environ['SINGULARITY_BINDPATH']}"
                )
        if "PYTHONPATH" in os.environ:
            os.environ["PYTHONPATH"] = "$LINC_DATA_ROOT/scripts:" + os.environ["PYTHONPATH"]
        else:
            os.environ["PYTHONPATH"] = "$LINC_DATA_ROOT/scripts"

    def setup_toil_directories(self, workdir: str) -> tuple[str, str]:
        dir_coordination = os.path.join(workdir, "coordination")
        try:
            os.mkdir(dir_coordination)
        except FileExistsError:
            print("Coordination directory already exists, not overwriting.")

        dir_slurmlogs = os.path.join(get_container_env_var("LOGSDIR"), "slurmlogs")
        try:
            os.mkdir(dir_slurmlogs)
        except FileExistsError:
            print("Slurm log directory already exists, not overwriting.")

        return (dir_coordination, dir_slurmlogs)


def ddf_pipeline(
    mspath: Annotated[str, Parameter(help="Directory where MSes are located.")],
    config_file: Annotated[str, Parameter(help="Configuration file containing the ddf-pipeline settings.")],
    scheduler: Annotated[
        str,
        Parameter(help="System scheduler to use."),
    ] = "singleMachine",
    rundir: Annotated[
        str,
        Parameter(help="Directory to run in."),
    ] = os.getcwd(),
    outdir: Annotated[
        str,
        Parameter(help="Directory to move outputs to."),
    ] = os.getcwd(),
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
    slurm_cores: Annotated[
        int,
        Parameter(help="Slurm cores to reserve."),
    ] = 32,
    slurm_memory: Annotated[
        Optional[int],
        Parameter(help="Slurm memory to reserve."),
    ] = None,
    restart: Annotated[
        bool,
        Parameter(help="Restart an existing DDF-pipeline run."),
    ] = False,
    use_node_scratch: Annotated[
        bool,
        Parameter(
            help="[slurm] this will run the pipeline on the node's local $TMPDIR instead of a user-defined rundir."
        ),
    ] = False,
):
    logger.info("Starting ddf-pipeline")
    args = locals()
    if args["restart"]:
        raise NotImplementedError("Restarting ddf runs is not yet supported.")
    config = DDFConfig(
        args["mspath"],
        os.path.abspath(args["config_file"]),
        outdir=outdir,
    )
    unneeded_keys = [
        "mspath",
        "scheduler",
        "rundir",
        "slurm_queue",
        "slurm_account",
        "slurm_time",
        "slurm_cores",
        "slurm_memory",
        "restart",
        "outdir",
        "use_node_scratch",
    ]
    args_for_linc = args.copy()
    for key in unneeded_keys:
        args_for_linc.pop(key)
    if args["use_node_scratch"]:
        args["rundir"] = "$TMPDIR"
    config.run_workflow(
        scheduler=args["scheduler"],
        slurm_params={
            "queue": args["slurm_queue"],
            "account": args["slurm_account"],
            "time": args["slurm_time"],
            "cores": args["slurm_cores"],
            "memory": args["slurm_memory"],
        },
        workdir=args["rundir"],
        restart=args["restart"],
        use_node_scratch=args["use_node_scratch"],
    )
