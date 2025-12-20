#!/usr/bin/env python
import cyclopts

from . import linc_runner, vlbi_runner, ugmrt_runner


def main():
    app = cyclopts.App()
    app.command(linc_runner.app, name="linc")
    app.command(vlbi_runner.app, name="vlbi")
    app.command(ugmrt_runner.app, name="ugmrt")

    app()


if __name__ == "__main__":
    main()
# vim: ft=python
