
./sinit

./sbatch test.slurm -J testing123
./sbatch test.slurm -c 2
./sbatch test.slurm -c 4
./sbatch test.slurm -c 4
./sbatch test.slurm -c 8
./sbatch test.slurm -J moretesting123
./sbatch test.slurm
./sbatch test.slurm
./sbatch test.slurm
./sbatch test.slurm
./sbatch test.slurm -c 14
./sbatch test.slurm --mem 16G
./sbatch test.slurm --mem 9M


