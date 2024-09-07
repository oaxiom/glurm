
def convert_seconds(seconds):
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
    return {'b': b, 'k': f'{k:,.2f}', 'M': f'{m:,.2f}', 'G': f'{g:,.2f}'}