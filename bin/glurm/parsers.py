


def parse_supported_SBATCH(list_of_SBATCH, args, log):

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
            log.warning('#SBATCH directive is not supported: {line}')

    return

def parse_exports(args_exports) -> dict:

    t = args_exports.split(',')
    new_export = {}
    
    for value in t:
        if value == 'ALL': # We assume ALL in the default case
            pass # Don't add it
            
        elif '=' in value: # Variables to pass to the script
            kv = value.split('=')
            new_export[kv[0].strip()] = kv[1].strip()
            # TODO: Deal with improperly formed exports
        
    return new_export