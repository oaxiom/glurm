
# Intro

Hi there kids! 

Would you like to run slurm?

Do you need to run it on a single computer that has no network access? 

Do you want it to be in pure python with no dependencies at all? 

Do you want lots of missing features that may or may not be implemented in the future?

Do you want lots of potential bugs in this non-battle hardened code?

If you answered yes to all of the above, then have I got the software for you!

Introducing glurm! The drop in slurm replacement!

Advantages:
- Pure python, absolutely no dependencies past python3!
- CLI match for slurm!
- Fast and simple setup!
- Exclamations are free!
- Uh... that's it.

Disadvantages.
- Single computer only.
- Single account only.
- Only supports CPU and memory allocation
- Many missing slurm features!
- Will never be fully feature complete!

# Running

To run, on a fresh computer:

sinit 

This will guess your setup based on your current computer. There are no config files, because config files are for amateurs! Uh...

Then run sbatch, squeue and sinfo as you would for slurm.

That's it! 

sbatch supports the following directives:

```
usage: sbatch [-h] [-c CPUS_PER_TASK] [-e ERROR] [--export EXPORT] [-J JOB_NAME] [--mem MEM] [-o OUTPUT] script

Slurm-like Glurm, single node job schedule emulator.

required arguments:
  script                slurm script to run

options:
  -h, --help            show this help message and exit
  -c CPUS_PER_TASK, --cpus-per-task CPUS_PER_TASK
                        number of cpus required per task, default=1
  -e ERROR, --error ERROR
                        File for batch script's standard error
  --export EXPORT       Specify environment variables to export
  -J JOB_NAME, --job-name JOB_NAME
                        Name of job
  --mem MEM             Minimum amount of real memory required, default = 0b, use b, k, M, G suffixes to specify memory
  -o OUTPUT, --output OUTPUT
                        file for batch script's standard output

Example usage: sbatch script.slurm
```

```



```




MIT license. Use at your own risk
