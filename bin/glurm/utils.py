
import errno
import os

def convert_seconds(seconds) -> str:
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    return "%d:%02d:%02d" % (hour, minutes, seconds)

def bytes_convertor(b:int) -> str:
    # Report the disk space du -hs style for a directory
    KB = 1024
    k = b / KB**1
    m = k / KB**1
    g = m / KB**1
    return {'b': b, 'k': f'{k:,.1f}', 'M': f'{m:,.1f}', 'G': f'{g:,.1f}'}

def bytes_convertor_f(b:int) -> str:
    # Report the disk space du -hs style for a directory
    # Return floats
    KB = 1024
    k = b / KB**1
    m = k / KB**1
    g = m / KB**1
    return {'b': b, 'k': k, 'M': m, 'G': g}

def get_memory():
    from .platform import LINUX, OSX, WINDOWS # Supported
    import subprocess

    if LINUX:
        return 10000000000 # free ?
    elif OSX:
        res = subprocess.run('sysctl -a  | grep hw.memsize', shell=True, capture_output=True)
        return int(res.stdout.decode().split(':')[1].strip())

def bytes_convertor2(mem:str) -> int:
    KB = 1024

    if mem.endswith('b'):
        return int(mem.strip('b'))

    if mem.endswith('k'):
        return int(mem.strip('k')) * KB

    elif mem.endswith('M'):
        return int(mem.strip('M')) * KB * KB

    elif mem.endswith('G'):
        return int(mem.strip('G')) * KB * KB * KB

    try:
        return int(mem)
    except ValueError:
        return 0

    return 0 # invalid value?

def pid_exists(pid:int) -> bool:
    """
    Check whether pid exists in the current process table.
    """
    if pid < 0:
        return False
    if pid == 0:
        # According to "man 2 kill" PID 0 refers to every process
        # in the process group of the calling process.
        # On certain systems 0 is a valid PID but we have no way
        # to know that in a portable fashion.
        raise ValueError('invalid PID 0')

    try:
        os.kill(pid, 0) # Yes, that's correct.
    except OSError as err:
        if err.errno == errno.ESRCH: # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM: # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True
