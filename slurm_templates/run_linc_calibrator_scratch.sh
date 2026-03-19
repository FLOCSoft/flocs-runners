sbatch <<EOT
#!/usr/bin/bash
#SBATCH -N 1 -c 32 -t 24:00:00 -J LINC_calibrator
cd \$TMPDIR
RUNDIR=\$(mktemp -d -p \$PWD)
cd \$RUNDIR

flocs-run linc calibrator --runner cwltool --rundir \$PWD --solveralgorithm directioniterative $(realpath $1)

cd \$TMPDIR
rsync -avP \$RUNDIR/LINC_calib* $(realpath $2)
rm -rf \$RUNDIR
EOT
