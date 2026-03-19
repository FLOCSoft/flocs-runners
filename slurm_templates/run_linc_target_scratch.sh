sbatch <<EOT
#!/usr/bin/bash
#SBATCH -c 40 -t 48:00:00 -p normal,infinite -J LINC_target
cd \$TMPDIR
RUNDIR=\$(mktemp -d -p \$PWD)
cd \$RUNDIR

flocs-run linc target --runner cwltool --rundir \$PWD --solveralgorithm directioniterative --cal-solutions $(realpath $1) $(realpath $2)

cd \$TMPDIR
rsync -avP \$RUNDIR/LINC_target* $(realpath $3)
rm -rf \$RUNDIR
EOT
