#!/usr/bin/env python
from cyclopts import Parameter
from typing import Annotated, Iterable
import cyclopts
import subprocess

app = cyclopts.App()


class FlocsDB:
    pass


@app.command()
def create(
    dbname: Annotated[str, Parameter(help="Directory where MSes are located.")],
    table_name: Annotated[
        str, Parameter(help="Directory where MSes are located.")
    ] = "processing_flocs",
    pipelines: Annotated[Iterable[str], Parameter(help="")] = ["linc"],
):
    pipelines = list(map(str.lower, pipelines))
    dbstr = f"create table {table_name}(source_name text default NULL"

    if "linc" in pipelines:
        dbstr += ", sas_id_calibrator1 text default NULL, sas_id_calibrator2 text default NULL, sas_id_calibrator_final text default NULL, sas_id_target text primary key default NULL, status_calibrator1 smallint default 0, status_calibrator2 smallint default 0, status_target smallint default 0"
    dbstr += ");"

    cmd = ["sqlite3", dbname, dbstr]
    print(f"Creating table via: {" ".join(cmd)}")

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to create table {table_name} in database {dbname}.")


def main():
    app()


if __name__ == "__main__":
    main()
