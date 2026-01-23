#!/usr/bin/env python
from cyclopts import Parameter
from typing import Annotated
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
):
    cmd = [
        "sqlite3",
        dbname,
        f"create table {table_name}(source_name text, sas_id_calibrator1 text, sas_id_calibrator2 text, sas_id_calibrator_final text, sas_id_target text primary key, status_calibrator1 smallint, status_calibrator2 smallint, status_target smallint);",
    ]

    return_code = subprocess.run(cmd)
    if not return_code:
        raise RuntimeError(f"Failed to create table {table_name} in database {dbname}.")

def main():
    app()

if __name__ == "__main__":
    main()
