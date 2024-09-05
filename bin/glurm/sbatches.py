


def parse_supported_SBATCH(list_of_SBATCH, args):

    for line in list_of_SBATCH:
        line = line.lstrip('#SBATCH ').strip()

        if line.startswith('--partition'):
            pass

        elif line.startswith('-N') or line.startswith('--nodes'):
            pass

        elif line.startswith('-c') or line.startswith('--cpus-per-task'):
            args.cpus_per_task = int(line.split(' '))[1]

        elif line.startswith('-J') or line.startswith('--job-name'):
            args.job_name = ' '.join(line.split(' ')[1:])

        elif line.startswith('-o') or line.startswith('--output'):
            args.output = line.split(' ')[1]

        # TODO:
        # -e, --error

        else:
            print('#SBATCH directive is not supported: {line}')

    return
