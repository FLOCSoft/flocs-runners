from .ddf_model import ensure_compatible_dr3_model
from .utils import (
    add_slurm_skeleton_ddf,
    extract_obsid_from_ms,
    get_container_env_var,
)
import glob
import configparser
import hashlib
import json
from http.client import HTTPException
import os
import shlex
import shutil
import structlog
import subprocess
import sys
import tarfile
import tempfile
import zlib
from pathlib import Path
from time import gmtime, sleep, strftime
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError
import numpy as np
from casacore.tables import table
from astropy.io import fits
from cyclopts import Parameter
from typing_extensions import Annotated, Optional, TypedDict

logger = structlog.getLogger()


class _DR3ArchiveEntry(TypedDict):
    key: str
    checksum: str
    size: int
    links: dict[str, str]


class DDFConfig:
    """Prepare and run a DDF-pipeline job."""

    def __init__(
        self,
        mspath: str,
        ddfconfig: str,
        outdir: str = os.getcwd(),
    ):
        self.outdir = str(Path(outdir).resolve())
        self.mspath = mspath
        self.ddfconfig = os.path.abspath(ddfconfig)

        filedir = os.path.join(os.path.abspath(mspath), "*pre-cal.ms")
        logger.info(f"Searching {filedir}")
        files = sorted(glob.glob(filedir))
        self.msfiles = files
        logger.info(f"Found {len(files)} files")
        if not len(files):
            logger.critical("No input MSes found")
            sys.exit(-1)

        try:
            self.obsid = extract_obsid_from_ms(files[0])
        except IndexError:
            self.obsid = "unknown"

    @staticmethod
    def _file_digest(path: Path, algorithm: str) -> str:
        digest = 1 if algorithm == "adler32" else hashlib.new(algorithm)
        with open(path, "rb") as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                if algorithm == "adler32":
                    digest = zlib.adler32(block, digest)
                else:
                    digest.update(block)
        return f"{digest:08x}" if algorithm == "adler32" else digest.hexdigest()

    def _fetch_dr3_archive(self, entry: _DR3ArchiveEntry, cache: Path) -> Path:
        """Return a verified local copy of a LoTSS-DR3 archive.

        Reuse a cached file if its size and checksum match the repository metadata.
        Otherwise, fetch it from SURF and apply the same checks before caching it.
        """
        archive = cache / entry["key"]
        algorithm, expected = entry["checksum"].split(":", 1)
        if archive.is_file():
            if archive.stat().st_size == entry["size"] and self._file_digest(archive, algorithm) == expected:
                return archive
            logger.warning(f"Removing invalid cached DR3 archive: {archive}")
            archive.unlink()
        temporary = None
        try:
            while True:
                response = urlopen(entry["links"]["content"], timeout=60)
                if response.status != 202:
                    break
                delay = response.headers.get("Retry-After", "60")
                response.close()
                logger.info(f"Waiting for SURF to restore {entry['key']} from tape")
                sleep(max(1, float(delay)))
            with response:
                if response.status != 200:
                    raise ValueError(f"Unexpected SURF download status: {response.status}")
                digest = 1 if algorithm == "adler32" else hashlib.new(algorithm)
                size = 0
                with tempfile.NamedTemporaryFile(dir=cache, suffix=".part", delete=False) as stream:
                    temporary = Path(stream.name)
                    for block in iter(lambda: response.read(8 * 1024 * 1024), b""):
                        stream.write(block)
                        if algorithm == "adler32":
                            digest = zlib.adler32(block, digest)
                        else:
                            digest.update(block)
                        size += len(block)
                        if size > entry["size"]:
                            raise ValueError(f"SURF download exceeds its published size: {entry['key']}")
                actual = f"{digest:08x}" if algorithm == "adler32" else digest.hexdigest()
                if size != entry["size"] or actual != expected:
                    raise ValueError(f"Downloaded DR3 archive failed its size/checksum: {entry['key']}")
            os.replace(temporary, archive)
            return archive
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def fetch_dr3_model(self):
        """Fetch complete native model paths for the exact MS field; raise if unavailable.

        Existing tape archives are recalled and waited for. An absent field, failed
        download or incomplete product set leaves full mode selected.
        Verified archives are reused; the required products are extracted each time.
        """
        names = set()
        for ms in self.msfiles:
            with table(ms + "::OBSERVATION", readonly=True, ack=False) as observation:
                for row in range(observation.nrows()):
                    target = observation.getcell("LOFAR_TARGET", row)
                    names.update(str(value).strip() for value in np.asarray(target).ravel())
        if len(names) != 1:
            raise ValueError(f"DR3 reuse requires one exact field name; found {sorted(names)}")
        field = names.pop()
        query = urlencode({"q": f'"{field}"', "size": 100})
        with urlopen("https://repository.surf.nl/api/records?" + query, timeout=30) as response:
            hits = json.load(response)["hits"]["hits"]
        matches = [record for record in hits
                   if record.get("custom_fields", {}).get("collection:metadata", {}).get("id") == field
                   and record["custom_fields"]["collection:metadata"].get("$schema")
                   == "https://repository.surf.nl/schema/lotss-dr3"]
        if len(matches) != 1:
            raise ValueError(f"Expected one exact SURF DR3 record for {field}; found {len(matches)}")
        with urlopen("https://repository.surf.nl/api/records/" + matches[0]["id"], timeout=30) as response:
            record = json.load(response)
        if record.get("custom_fields", {}).get("collection:metadata", {}).get("id") != field:
            raise ValueError("SURF record field differs from the requested MS field")
        # Never use an archive-provided path for extraction or a different/nearest field.
        cache = Path(self.outdir).resolve() / ".dr3-models" / record["id"]
        cache.mkdir(parents=True, exist_ok=True)
        stem = "image_full_ampphase_di_m.NS"
        required = [stem + ".DicoModel", stem + ".mask01.fits", stem + ".app.restored.fits",
                    "image_dirin_SSD_m.npy.ClusterCat.npy"]
        entries = record.get("files", {}).get("entries", {})
        missing = {"images.tar", "uv_misc.tar"} - entries.keys()
        if missing:
            raise ValueError(f"Missing DR3 archives for {field}: {sorted(missing)}; full DDF is required")
        # Start both recalls before waiting on either missing archive.
        for name in ("images.tar", "uv_misc.tar"):
            if (cache / name).is_file():
                continue
            try:
                request = Request(entries[name]["links"]["content"], headers={"Range": "bytes=0-0"})
                with urlopen(request, timeout=60):
                    pass
            except (OSError, URLError, HTTPException) as error:
                logger.warning(f"DR3 recall request for {name} failed; will retry at download: {error}")
        extracted = set()
        for name in ("images.tar", "uv_misc.tar", "misc.tar"):
            if name not in entries:
                continue
            logger.info(f"Fetching DR3 {name} for exact field {field}")
            try:
                archive = self._fetch_dr3_archive(entries[name], cache)
                with tarfile.open(archive, "r:") as bundle:
                    members = {}
                    for member in bundle:
                        basename = Path(member.name).name
                        if basename in required:
                            if basename in members or not member.isfile():
                                raise ValueError(f"Ambiguous or non-regular DR3 model member: {member.name}")
                            members[basename] = member
                    for basename, member in members.items():
                        staged = None
                        try:
                            with bundle.extractfile(member) as source:
                                with tempfile.NamedTemporaryFile(dir=cache, delete=False) as target:
                                    staged = Path(target.name)
                                    shutil.copyfileobj(source, target)
                            if staged.stat().st_size != member.size:
                                raise ValueError(f"Incomplete extracted model file: {basename}")
                            os.replace(staged, cache / basename)
                            extracted.add(basename)
                        finally:
                            if staged is not None:
                                staged.unlink(missing_ok=True)
            except (OSError, URLError, HTTPException, ValueError, tarfile.TarError) as error:
                logger.warning(f"DR3 {name} unavailable for {field}: {error}")
            if set(required) <= extracted:
                break
        if not set(required) <= extracted:
            raise ValueError(f"Incomplete DR3 products for {field}: missing {sorted(set(required) - extracted)}")

        with fits.open(cache / required[2], memmap=True) as image, fits.open(cache / required[1], memmap=True) as mask:
            header = image[0].header
            for key in ("NAXIS1", "NAXIS2", "CRVAL1", "CRVAL2", "CRPIX1", "CRPIX2", "CDELT1", "CDELT2"):
                if not np.isclose(header[key], mask[0].header[key], rtol=0, atol=1e-9):
                    raise ValueError(f"DR3 image/mask geometry differs: {key}")
            with table(self.msfiles[0] + "::FIELD", readonly=True, ack=False) as msfield:
                ra, dec = np.degrees(np.asarray(msfield.getcell("PHASE_DIR", 0)).reshape(-1, 2)[0])
            from astropy.coordinates import SkyCoord
            centre = SkyCoord(header["CRVAL1"], header["CRVAL2"], unit="deg")
            if SkyCoord(ra, dec, unit="deg").separation(centre).arcsec > 1:
                raise ValueError("DR3 model phase centre differs from the input MS")
        clusters = np.load(cache / required[3], allow_pickle=False)
        if not len(clusters) or not clusters.dtype.names or not {"ra", "dec"} <= set(clusters.dtype.names):
            raise ValueError("Invalid DR3 cluster catalogue")
        return {"field": field, "cache": cache, "model": cache / required[0],
                "image": cache / required[2], "mask": cache / required[1], "clusters": cache / required[3]}

    def prepare_dr3_config(self):
        """Choose lite/full and write the configuration submitted to DDF."""
        original = Path(self.ddfconfig)
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        with original.open() as stream:
            config.read_file(stream)
        if config.get("inputmodel", "basedicomodel", fallback="").strip().lower() not in ("", "none"):
            logger.info("Using the explicitly configured input sky model")
            return
        mode, reason = "full", "No complete usable exact-field DR3 model"
        try:
            products = self.fetch_dr3_model()
            if products:
                model = products["model"]
                compatible = model.with_name(model.stem + ".compatible.DicoModel")
                try:
                    information = ensure_compatible_dr3_model(model, compatible)
                except Exception as error:
                    # Model decoding and serialization can raise library-specific errors.
                    raise ValueError(f"Could not prepare DR3 model {model}: {error}") from error
                logger.info("Prepared DR3 model", **information)
                if (config.getint("image", "imsize", fallback=20000) != information["npix"]
                        or not np.isclose(config.getfloat("image", "cellsize", fallback=1.5), information["cell"])):
                    raise ValueError("Requested image size/pixel scale differs from the DR3 model")
                if config.get("image", "phasecenter", fallback="").strip().lower() not in ("", "none"):
                    raise ValueError("Explicit image phase centre supplied; using a full run")
                if not config.has_section("inputmodel"):
                    config.add_section("inputmodel")
                config.set("inputmodel", "basedicomodel", str(compatible.with_suffix("")))
                config.set("inputmodel", "basemaskname", str(products["mask"]))
                config.set("inputmodel", "baseimagename", str(products["image"])[:-len(".app.restored.fits")])
                if not config.has_section("image"):
                    config.add_section("image")
                config.set("image", "clusterfile", str(products["clusters"]))
                mode, reason = "lite", f"Complete validated SURF DR3 model for {products['field']}"
        except (OSError, URLError, HTTPException, EOFError, ValueError, KeyError, tarfile.TarError) as error:
            reason = str(error)
            logger.warning(f"DR3 model reuse unavailable; selecting full DDF run: {reason}")
        if not config.has_section("data"):
            config.add_section("data")
        config.set("data", "colname", "DATA")
        for section, options in {"control": ("bootstrap", "polcubes", "stokesv"), "spectra": ("do_dynspec",)}.items():
            if not config.has_section(section):
                config.add_section(section)
            for option in options:
                config.set(section, option, "False")
        Path(self.outdir).mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix=f"DDF-inputs_L{self.obsid}.", dir=os.path.abspath(self.outdir)))
        generated = directory / "ddf-input.cfg"
        with generated.open("w") as stream:
            config.write(stream)
        self.ddfconfig = str(generated)
        logger.info(f"Selected DDF {mode} mode: {reason}; config: {generated}")

    def prepare_run_inputs(self, workdir):
        """Save the selected configuration and cluster catalogue with the run."""
        original = Path(self.ddfconfig)
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        with original.open() as stream:
            config.read_file(stream)
        root = Path(workdir).resolve()
        model = config.get("inputmodel", "basedicomodel", fallback="").strip()
        clusterfile = config.get("image", "clusterfile", fallback="").strip()
        if model.lower() not in ("", "none") and clusterfile.lower() not in ("", "none"):
            cluster = Path(clusterfile)
            if not cluster.is_absolute():
                cluster = root / cluster
            saved_cluster = root / "image_dirin_SSD_m.npy.ClusterCat.npy"
            if cluster.resolve() != saved_cluster.resolve():
                shutil.copy2(cluster, saved_cluster)
            config.set("image", "clusterfile", saved_cluster.name)
        generated = root / "ddf-input.cfg"
        with generated.open("w") as stream:
            config.write(stream)
        self.ddfconfig = str(generated)

    @staticmethod
    def prepare_solution_links(workdir):
        """Make internal solution links relative before moving the run directory."""
        root = Path(workdir).resolve()
        for link in (root / "SOLSDIR").rglob("*.npz"):
            if link.is_symlink():
                target = Path(os.readlink(link))
                if target.is_absolute() and target.is_relative_to(root):
                    if not link.is_file():
                        raise FileNotFoundError(f"Missing DDF solution: {link} -> {target}")
                    link.unlink()
                    link.symlink_to(os.path.relpath(target, link.parent))

    def move_results_from_rundir(self):
        self.prepare_solution_links(self.rundir)
        outpath = self.get_results_path()
        logger.info(f"Moving results from {self.rundir} to: {outpath}")
        shutil.move(self.rundir, outpath)

    def get_results_path(self):
        date = strftime("%Y_%m_%d-%H_%M_%S", gmtime())
        return os.path.abspath(os.path.join(self.outdir, f"DDF-pipeline_L{self.obsid}_{date}"))

    def submit_restart(self, ddf_container: str, slurm_params: dict):
        command = ["apptainer", "exec", ddf_container, "pipeline.py", self.ddfconfig]
        log_path = os.path.join(self.rundir, "log_DDF-pipeline.txt")
        outpath = self.get_results_path()

        directives = []
        if slurm_params.get("time"):
            directives.append(f"#SBATCH -t {slurm_params['time']}")
        if slurm_params.get("cores"):
            directives.append(f"#SBATCH -c {slurm_params['cores']}")
        if slurm_params.get("queue"):
            directives.append(f"#SBATCH -p {slurm_params['queue']}")
        if slurm_params.get("account"):
            directives.append(f"#SBATCH -A {slurm_params['account']}")
        if slurm_params.get("memory"):
            directives.append(f"#SBATCH --mem {slurm_params['memory']}GB")
        directives.append(f"#SBATCH --job-name DDF-pipeline_L{self.obsid}")

        script = "\n".join(
            [
                "#!/bin/bash",
                *directives,
                f"cd {shlex.quote(self.rundir)} || exit $?",
                f"{shlex.join(command)} > {shlex.quote(log_path)} 2>&1",
                "status=$?",
                "if [ \"$status\" -eq 0 ]; then",
                "    cd / || exit $?",
                f"    mv -- {shlex.quote(self.rundir)} {shlex.quote(outpath)} || exit $?",
                "fi",
                "exit \"$status\"",
                "",
            ]
        )
        with open("temp_jobscript.sh", "w") as jobscript:
            jobscript.write(script)
        logger.info("Written restart jobscript to temp_jobscript.sh")
        out = subprocess.check_output(["sbatch", "temp_jobscript.sh"]).decode("utf-8")
        print(out)

    def run_restart(self, ddf_container: str, scheduler: str, slurm_params: dict):
        command = ["apptainer", "exec", ddf_container, "pipeline.py", self.ddfconfig]
        if scheduler in {"slurm", "slurm-srun"}:
            self.submit_restart(ddf_container, slurm_params)
            return
        if scheduler != "singleMachine":
            raise ValueError(f"Unsupported scheduler `{scheduler}` provided.")

        log_path = os.path.join(self.rundir, "log_DDF-pipeline.txt")
        logger.info(f"Restarting DDF-pipeline in {self.rundir} with command:\n{shlex.join(command)}")
        with open(log_path, "wb") as log_file:
            subprocess.run(command, cwd=self.rundir, stdout=log_file, stderr=subprocess.STDOUT, check=True)
        self.move_results_from_rundir()

    def run_workflow(
        self,
        scheduler: str = "slurm",
        workdir: str = os.getcwd(),
        outdir: Optional[str] = None,
        slurm_params: dict = {},
        restart: bool = False,
        use_node_scratch: bool = False,
    ):
        batch = scheduler in {"slurm", "slurm-srun"}
        if not batch and scheduler != "singleMachine":
            raise ValueError(f"Unsupported scheduler `{scheduler}` provided.")

        if outdir is not None:
            self.outdir = str(Path(outdir).resolve())
        workdir = str(Path(workdir).resolve())
        if batch and not restart:
            self.rundir = tempfile.mkdtemp(prefix=f"tmp.DDF-pipeline_L{self.obsid}.", dir=workdir)
        else:
            self.rundir = workdir

        if restart:
            if not os.path.isdir(self.rundir):
                raise NotADirectoryError(f"DDF restart directory does not exist: {self.rundir}")
            logger.info(f"Attempting to restart existing run from {self.rundir}.")
            prepared = Path(self.rundir) / "ddf-input.cfg"
            if prepared.is_file():
                self.ddfconfig = str(prepared)
        self.setup_apptainer_variables(self.rundir)
        ddf_container = self.verify_ddf_container()
        logger.info(f"Running DDF-pipeline under {scheduler} in {self.rundir}")

        if restart:
            self.run_restart(ddf_container, scheduler, slurm_params)
            return

        self.prepare_dr3_config()
        self.prepare_run_inputs(self.rundir)

        if batch:
            Path(self.outdir).mkdir(parents=True, exist_ok=True)
            wrapped_cmd = add_slurm_skeleton_ddf(
                ddf_container=ddf_container,
                data_dir=os.path.abspath(self.mspath),
                workdir=self.rundir,
                outdir=self.outdir,
                obsid=self.obsid,
                local_scratch=use_node_scratch,
                **slurm_params,
            )
            script = Path(self.rundir) / "temp_jobscript.sh"
            script.write_text(wrapped_cmd)
            logger.info(f"Written temporary jobscript to {script}")
            out = subprocess.check_output(["sbatch", str(script)]).decode("utf-8")
            print(out)
            return

        root = Path(self.rundir)
        msfiles = sorted(root.glob("*pre-cal.ms"))
        if not msfiles:
            raise FileNotFoundError(f"No input MeasurementSets in {self.rundir}")
        cmd = ["apptainer", "exec", ddf_container, "make_mslists.py", "force"]
        logger.info(f"Running command:\n{shlex.join(cmd)}")
        subprocess.run(cmd, cwd=self.rundir, check=True)
        (root / "big-mslist.txt").write_text("".join(f"{ms.name}\n" for ms in msfiles))

        cmd = ["apptainer", "exec", ddf_container, "pipeline.py", self.ddfconfig]
        logger.info(f"Running command:\n{shlex.join(cmd)}")
        with (root / "log_DDF-pipeline.txt").open("wb") as log_file:
            subprocess.run(cmd, cwd=self.rundir, stdout=log_file, stderr=subprocess.STDOUT, check=True)
        if root.resolve() != Path(self.outdir).resolve():
            self.move_results_from_rundir()

    def verify_ddf_container(self) -> str:
        ddf_container = os.path.abspath(os.path.join(os.environ["CWL_SINGULARITY_CACHE"], "ddf-pipeline.sif"))
        if not os.path.isfile(ddf_container):
            logger.critical("Container `ddf-pipeline.sif` not found in $CWL_SINGULARITY_CACHE")
            raise RuntimeError("No viable ddf-pipeline container was found.")
        return ddf_container

    def setup_apptainer_variables(self, workdir):
        try:
            out = subprocess.check_output(["singularity", "--version"]).decode("utf-8").strip()
        except (FileNotFoundError, subprocess.CalledProcessError):
            out = subprocess.check_output(["apptainer", "--version"]).decode("utf-8").strip()
        if "apptainer" in out:
            if "APPTAINER_BINDPATH" not in os.environ:
                os.environ["APPTAINER_BINDPATH"] = f"{os.path.dirname(workdir)}"
            else:
                os.environ["APPTAINER_BINDPATH"] = f"{workdir}" + f",{os.environ['APPTAINER_BINDPATH']}"
        elif "singularity" in out:
            if "SINGULARITY_BINDPATH" not in os.environ:
                os.environ["SINGULARITY_BINDPATH"] = f"{os.path.dirname(workdir)}"
            else:
                os.environ["SINGULARITY_BINDPATH"] = (
                    f"{os.path.dirname(workdir)}" + f",{os.environ['SINGULARITY_BINDPATH']}"
                )

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
    config = DDFConfig(
        mspath,
        config_file,
        outdir=outdir,
    )
    config.run_workflow(
        scheduler=scheduler,
        slurm_params={
            "queue": slurm_queue,
            "account": slurm_account,
            "time": slurm_time,
            "cores": slurm_cores,
            "memory": slurm_memory,
        },
        workdir=rundir,
        restart=restart,
        use_node_scratch=use_node_scratch,
    )
