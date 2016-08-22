# vim: ai:si:number:ts=4:et:sw=4:st=4
"""
A module performing the merging of files during the experiment.
"""

import os
import logging
import time
import glob
import cPickle as pickle
import shutil
from subprocess import Popen, PIPE
from functools import wraps

############
# Settings #
############
# data
DATA_DIR = "/hera/sids/GO2014"
RSA51 = os.path.join(DATA_DIR, "RSA51")
RSA52 = os.path.join(DATA_DIR, "RSA52")
RSA30 = os.path.join(DATA_DIR, "RSA30")
OSC_DIR = os.path.join(DATA_DIR, "Oscil")
OSC_CHANS = ("C1", "C2", "C3", "C4")
REF_CHAN = os.path.join(OSC_DIR, "C2")
# path to time2root
T2R = "/data.local2/time2root/time2root"
OUTPUT_DIR = os.path.join(DATA_DIR, "ROOT")
LOGFILE = os.path.join(DATA_DIR, "Merger", "merging.log")
PROCESS = os.path.join(DATA_DIR, "Merger", "processed.list")
PERIOD = 30  # seconds


class TimeExtractor(object):
    """
    A collection of methods used to extract times from various instruments.
    """
    osc_time = "%Y.%m.%d.%H.%M.%S"
    rsa50_time = "%Y.%m.%d.%H.%M.%S.%f.TIQ"
    rsa30_time = "%Y%m%d-%H%M%S"

    @classmethod
    def rsa30(cls, name):
        """Extract time from RSA30 IQT files."""
        name = name.split('/')[-1]
        name = name.split('-')[:-1]
        name = "-".join(name)
        return time.strptime(name, cls.rsa30_time)

    @classmethod
    def rsa50(cls, name):
        """Extract time from RSA50 TIQ files."""
        name = name.split('/')[-1]
        name = name.split('-')[1]
        return time.strptime(name, cls.rsa50_time)

    @classmethod
    def osc(cls, name):
        """Extract time from LeCroy CSV files."""
        name = name.split('/')[-1]
        name = name.split('_')[1]
        return time.strptime(name, cls.osc_time)


def dir_restore(func):
    """Changes back to start directory, regardless of what
    a function does inside it."""

    @wraps(func)
    def decorator(*ar, **kw):
        previous = os.getcwd()
        result = func(*ar, **kw)
        os.chdir(previous)
        return result

    return decorator


@dir_restore
def get_injections(processed):
    """
    Find and return new injections. Injections are classified according to
    their starting point in time.

    Arg:
        processed (set): a collection of previously processed injections.

    Returns:
        A list of tuples, where each tuple contains the starting time of the
        injection and the starting time of the next injection.
    """
    os.chdir(REF_CHAN)

    files_list = glob.iglob("C2*inj.csv")
    all_times_list = (TimeExtractor.osc(f) for f in files_list)
    times_list = [f for f in all_times_list if f not in processed]
    times_list.sort(key=lambda x: time.mktime(x))
    # in case S/A file have not been copied, dont merge last injection.
    times_list = times_list[:-1]

    # creates tuples of 2 subsequent injection times
    # this is safe even in the case of only 1 entry in file_list:
    # slices will simply be empty - brilliant.
    interval_tuples = zip(times_list[:-1], times_list[1:])

    return interval_tuples


def create_range_predicate(start, stop, tolerance=0):
    """
    Create a filtering function that checks if time was within the expected
    time range.
    """
    start = time.mktime(start)
    stop = time.mktime(stop)
    if stop < start:
        raise ValueError("Start time is after stop time")

    def predicate(data_time):
        data_time = time.mktime(data_time)
        return start - tolerance < data_time < stop + tolerance

    return predicate


def check_output(n, message):
    """
    Decorator: check output of function, if function returns list of
    length n everything is alright, else use message to construct an
    error message.
    """

    def decorator(func):
        @wraps(func)
        def decorated(start, *args):
            data = func(start, *args)
            if len(data) != n:
                logging.error("Injection@%s: found %d %s files",
                              time.strftime("%m.%d.%H.%M.%S", start),
                              len(data), message)
                data = []
            return data

        return decorated

    return decorator


@check_output(4, "osc inj")
def get_inj_files(start):
    """Retrieve oscilloscope injection files"""
    data = []
    for channel in OSC_CHANS:
        glob_str = "{osc}/{ch}/{ch}_{tm}_*.csv".format(
            osc=OSC_DIR, ch=channel,
            tm=time.strftime(TimeExtractor.osc_time, start))
        found_files = glob.glob(glob_str)
        data.extend(found_files)
    return data


@check_output(4, "osc ext")
def get_ext_files(start, predicate):
    """Retrieve oscilloscope extraction files"""
    data = []
    for channel in OSC_CHANS:
        glob_str = "{osc}/{ch}/{ch}_*.csv".format(osc=OSC_DIR, ch=channel)
        found_files = [f for f in glob.glob(glob_str)
                       if predicate(TimeExtractor.osc(f))]
        data.extend(found_files)
    return data


def get_osc_files(start, predicate):
    """Retrieve oscilloscope files."""
    data = get_inj_files(start)
    data += get_ext_files(start, predicate)
    return data


@check_output(2, "rsa50")
def get_rsa50_files(start, predicate):
    data = []
    for rsa in (RSA51, RSA52):
        glob_str = "{rsa}/*.TIQ".format(rsa=rsa)
        found_files = [f for f in glob.glob(glob_str)
                       if predicate(TimeExtractor.rsa50(f))]
        data += found_files
    return data


@check_output(1, "rsa30")
def get_rsa30_files(start, predicate):
    glob_str = "{rsa}/*.iqt".format(rsa=RSA30)
    found_files = [f for f in glob.glob(glob_str)
                   if predicate(TimeExtractor.rsa30(f))]
    return found_files


@dir_restore
def merge(start, data, debug=False):
    """
    Merge the gathered files using time2root.
    """
    DEVNULL = open(os.devnull, 'wb')
    output_filename = time.strftime(TimeExtractor.osc_time, start) + ".root"
    # get absolute path to output files
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    # get absolute path to input files
    data = (os.path.abspath(file_) for file_ in data)
    # change directory to time2root dir
    os.chdir(os.path.dirname(T2R))
    for file_ in data:
        proc = Popen([T2R, output_path, file_], stdout=PIPE, stderr=PIPE)
        output, err = proc.communicate()
        out = proc.wait()
        if out != 0:
            ## Temporary fix to see if this helps
            logging.error("Injection@%s: T2R failed at %s with code %d",
                          time.strftime("%m.%d.%H.%M.%S", start),
                          file_.split('/')[-1],
                          out)
            logging.error("Error message and output: %s %s", output, err)
    DEVNULL.close()


def save_processed(filename, processed):
    """
    Pickle a data collection (set) and append it to a file.

    Args:
        filename (str): the name of the file to which to write.
        processed (set): the collection which will be appended to file.
    """
    with open(filename, "ab") as file_:
        pickle.dump(processed, file_, 0)


def get_processed(filename):
    """
    Unpickle all sets contained within a file.

    Arg:
        filename (str): the name of the file from which to read.

    Returns:
        A union of all sets contained within the file.
    """
    processed = set()
    try:
        with open(filename, "rb") as processed_file:
            while True:
                processed.update(pickle.load(processed_file))
    except EOFError:
        # expected
        pass
    except IOError:
        # if file doesn't exist will create a file with an empty set.
        save_processed(filename, processed)
    return processed


def loop(processed):
    """
    The program loop, made up of the following steps:

        1. Find all not processed injection files from oscilloscope
           (default: C2)
        2. Find S/A and extraction files belonging to each injection.
        3. Create a root file if all raw files have been found or log failure.

    Args:
        processed (set): a set of already processed injections as returned
                         by :py:func:`get_processed`.
    """
    injections = get_injections(processed)
    for start, stop in injections:
        predicate = create_range_predicate(start, stop)
        data2merge = []
        data2merge += get_osc_files(start, predicate)
        data2merge += get_rsa50_files(start, predicate)
        data2merge += get_rsa30_files(start, predicate)
        if len(data2merge) != 11:
            if time.mktime(stop) - time.mktime(start) > 1.5 * 60:
                logging.error("Injection@%s had next inj after "
                              "%d seconds",
                              time.strftime("%m.%d.%H.%M.%S", start),
                              time.mktime(stop) - time.mktime(start))
            logging.error("Injection@%s could not be merged",
                          time.strftime("%m.%d.%H.%M.%S", start))
        else:
            merge(start, data2merge)
            logging.info("Successfully merged injection@%s",
                         time.strftime("%m.%d.%H.%M.%S", start))
        processed.add(start)
        save_processed(PROCESS, set([start]))
    logging.info("Finished loop")


def backup_list():
    """Backup the list of processed injections to a different directory"""
    shutil.copy(PROCESS, "/hera/sids/")


def main():
    """
    The main function of the application. It consists of an application
    loop and sleeping till the end of time.
    """
    i = 0
    os.chdir(DATA_DIR)
    processed = get_processed(PROCESS)
    while True:
        try:
            loop(processed)
            backup_list()
        except Exception as exc:
            logging.exception("Something aweful happened!")
        time.sleep(PERIOD)
        print
        "Ping", i
        i += 1


def config_logging():
    """Set the parameters for the logfile."""
    logging.basicConfig(
        format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', filename=LOGFILE,
        level=logging.INFO)
    logging.info("====== Start operation ======")


if __name__ == "__main__":
    config_logging()
    main()
