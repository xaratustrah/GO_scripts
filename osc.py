"""Module dealing with the renaming and proper naming of the files saved
by the oscilloscope."""
import glob, time, os, logging


LIMIT = 1  # discriminating pulsewidth (fwhm) in microseconds
FAILED = set()

def parse_time(line):
    """Parse time from lecroy data and return time to use for filename."""
    # remove first and last columns
    line = line.split(',')[1]
    # parse time from file
    input_format = "%d-%b-%Y %H:%M:%S"
    time_str = time.strptime(line, input_format)
    output_format = "%Y.%m.%d.%H.%M.%S"
    # reformat time_str
    time_str = time.strftime(output_format, time_str)
    return time_str

def find_kind(data):
    """Determine kind of measurement (extraction/injection).
    Uses the length of the pulse as a method of determining what it is dealing
    with."""
    max_val = max(data, key=lambda x: x[1])[1]
    min_val = min(data, key=lambda x: x[1])[1]
    middle = (max_val+min_val)*0.5
    delta_t = data[1][0] - data[0][0]
    pulse_len = sum(1 if entry[1] > middle else 0 for entry in data)
    pulse_len *= delta_t
    return "ext" if pulse_len>LIMIT*1e-6 else "inj"

def read_data_and_time(file_):
    # skip first 3 lines
    for i in range(3):
        next(file_)
    # next line has time
    time_str = parse_time(next(file_))
    # next line is worthless
    next(file_)
    data = [tuple(map(float, line.split(','))) for line in file_]
    return data, time_str

def rename(old_name):
    """Rename saved file to the correct format."""
    with open(old_name) as fh:
        data, time_str = read_data_and_time(fh)
        # get kind of measurement based on pulse width
        kind = find_kind(data)
    channel = old_name[:2]
    new_name = "{ch}_{tm}_{tp}.{ext}".format(tm=time_str, ch=channel,
                                             tp=kind, ext="csv")
    os.rename(old_name, new_name)
    return new_name

def reject_new(file_list, limit=5):
    """Filter out files modified less than 5 seconds ago."""
    now = time.time()
    def access_time_filter(name):
        return abs(now - os.stat(name).st_mtime) > limit
    return filter(access_time_filter, file_list)

def rename_all(path):
    logger = logging.getLogger("autocopy.osc")
    # save start directory
    prev = os.getcwd()
    # go to local file directory
    os.chdir(path)
    # find all not-renamed files
    old_files_list = set(glob.glob("*Trace*.csv")) - FAILED
    old_files_list = reject_new(old_files_list)
    for old_name in old_files_list:
        try:
            new_name = rename(old_name)
            logger.info("Renamed '%s' to '%s'", old_name, new_name)
        except Exception as e:
            FAILED.add(old_name)
            logger.error("Caught exception while renaming '%s':\n%s:%s",
                         old_name, type(e), e)
    os.chdir(prev)
