
Hi there kids! 

Would you like to run slurm?

Do you need to run it on a single computer that has no network access? 

Do you want it to be in pure python with no dependencies at all? 

Do you want lots of missing features that may or may not be implemented in the future?

Do you want lots of potential bugs in this non-battle hardened code?

If you answered yes to all of the above, then have I got the software for you!

Introducing glurm! The drop in slurm replacement!

Advantages:
. Pure python, absolutely no dependencies past python3!
. CLI match for slurm!
. Fast and simple setup!
. Exclamations are free!
. Uh... that's it.

Disadvantages.
. Single computer only.
. Single account only.
. Only really supports CPU allocation
. Many missing slurm features.
. Will never be fully feature complete.

To run:

On a fresh computer:

sinit 

This will guess your setup based on your current computer. There are no config files, because config files are for amateurs! Uh...

Then run sbatch, squeue and sinfo as you would for slurm.

That's it! 

MIT license. Use at your own risk
