"""Perform a periodic backup of data. Tested with Python 3.4"""
import glob
import os
import posixpath
import time
import pickle
import logging
import threading
from subprocess import Popen, PIPE
from pprint import pformat
from collections import deque
import osc


##################
#    Settings    #
##################
PERIOD = 5  # seconds
THREAD_LIMIT = 2  # limit concurrently copied files
# Remote host
HOST = ""  # get your own
# Remote folder
REMOTE_FOLDER = "tmp"
# Folder to which data will be saved. Needs to exist.
PATH_TO_REMOTE = posixpath.join("%s:." % HOST, REMOTE_FOLDER)
# Folder with data to be uploaded to remote
PATH_TO_DATA = os.path.join("D:\\", "temp", "data_transfer", "folder")
# The name of the log file to be used by the logging module.
# Default location:
#   cwd
LOGFILE = "autocopy.log"
LOGFILE = os.path.join(os.getcwd(), LOGFILE)
# The list of files that have been copied already - uses pickle to prevent
# having to parse the log file
FILE_LIST = "file.list"
FILE_LIST = os.path.join(os.getcwd(), FILE_LIST)
# Path to the putty scp client
PSCP = "pscp"
# The glob string used to search for files - I recommend this one for the oscilloscope,
# for the S/A's you can use the basename, possibly with the file extension
GLOBSTR = "*_2014*.csv"
# logger, has to be defined here, but don't modify
logger = None
# leave this setting for spectrum analyzers, change to True for oscilloscope
rename = False
##################


def check_access(fname, limit=30):
    """Return True if last file modification was more than limit seconds ago."""
    last_mod = os.stat(os.path.join(PATH_TO_DATA, fname)).st_mtime
    now = time.time()
    last = now - last_mod
    if last > limit:
        return True
    else:
        logger.info("'%s' excluded, last access %d s ago.", fname, last)
        return False

def check_local():
    """Return a set of all files (not directories) in the path dir."""
    prev = os.getcwd()
    os.chdir(PATH_TO_DATA)
    # find all text files beginning with the current year
    file_list = set(glob.glob("{base}".format(base=GLOBSTR)))
    os.chdir(prev)
    return file_list

def check_remote():
    """Return a set of files in remote directory."""
    proc = Popen(["plink", "-ssh", HOST, "ls", REMOTE_FOLDER], shell=True,
                 stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    if proc.poll() != 0:
        logger.error("Error in ls: %s", err.decode('ascii').strip() if len(err)
                                         else out.decode('ascii').strip())
        import sys
        print("Can't access remote location. Aborting")
        sys.exit(1)
    return set(line.strip() for line in out.decode('ascii').splitlines())

def copy_file(fname):
    """Return Popen object that copies file from local DATA folder to REMOTE."""
    src = os.path.join(PATH_TO_DATA, fname)
    dest = posixpath.join(PATH_TO_REMOTE, fname)
    return Popen([PSCP, src, dest], shell=True, stdout=PIPE, stderr=PIPE), fname


class FileListBuilder:
    """Handler for saving and loading the set of processed files."""
    def __init__(self, name):
        self.filename = name

    def save_list(self, set_obj, mode="a"):
        """Save the list to file.
        Mode should be: "a" or "w" to append or overwrite."""
        with open(self.filename, mode + "b") as file_:
            pickle.dump(set_obj, file_)

    def read_list(self):
        """Read all data saved in file"""
        read_set = set()
        try:
            with open(self.filename, "rb") as file_:
                # unpickles until runs into EOFError
                while True:
                    read_set.update(pickle.load(file_))
        except (FileNotFoundError, EOFError):
            pass
        return read_set

    def get_processed(self):
        """Get processed list - chooses whether to use remote or local."""
        print("Getting list of processed files.")
        remote_list = check_remote()
        local_list = self.read_list()
        # if local_list is empty take remote
        if not local_list:    
            self.save_list(remote_list, "w")
            logger.info("Local list empty, taking remote list")
            processed = remote_list
        # if local_list is not empty, compare to remote
        else:
            # if local and remote differ by more than 5 entries, ask
            if len(local_list ^ remote_list) > 5:
                choices = {'l':local_list, "r":remote_list}
                choice = None
                while choice is None:
                    choice = input("Choose file list: l(ocal) or r(emote): ")
                    choice = choices.get(choice, None)
                self.save_list(choice, "w")
                logger.info("User chose list")
                processed = choice
            # if they don't, choose local, as the safer choice
            else:
                self.save_list(local_list, "w")
                logger.info("Choosing local list")
                processed = local_list
        return processed


def handle_process(in_tup, deq):
    """Append the filename to deque if copying it ends with a 0 output code."""
    proc, fname = in_tup
    outcome = proc.poll()
    if outcome is None:
        outcome = proc.wait()
    if outcome == 0:
        logger.info("Transferred file: '%s'", fname)
        deq.append(fname)
    else:
        logger.error("Error in file '%s', code %d", fname, outcome)
        logger.error("Error output: %s",
                      proc.stderr.read().decode("ascii").strip())

def transfer_files(files):
    """Transfer files and return list of successfully transf. ones."""
    # for each new file start copying (asynchronously)
    # creates a generator object that be used to late start the copying
    processes = (copy_file(file_) for file_ in files)
    # deque for gathering data from threads
    deq = deque()
    
    # threaded version
    threads = []
    def join_threads():
        # access to threads
        nonlocal threads
        # join all threads
        for thread in threads:
            thread.join()
        # clear list
        threads = []
    # wait for end of transfer
    for i, proc in enumerate(processes):
        if i % THREAD_LIMIT == 0:
            # join started threads before spawning new ones
            join_threads()
        thread = threading.Thread(target=handle_process,
                                  args=(proc, deq))
        thread.start()
        threads.append(thread)
    else:
        # will execute as the last statement
        join_threads()

    # serial version:
    # process = [proc for proc in processes]
    # for proc in processes:
    #     handle_process(proc, dq)
    transferred = set(deq)
    return transferred

def timing(func):
    """Decorator: prints function execution time."""
    def deco_func(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        stop = time.time()
        print("PING: Elapsed time {:4.2f}s".format(stop-start))
        return result
    return deco_func

@timing
def loop(processed, flb):
    """Main application loop."""
    if rename:
        # rename all files that have the default filenames
        osc.rename_all(PATH_TO_DATA)
    # get locally available files minus the transferred ones
    files = check_local().difference(processed)
    if files:
        logger.info("Found new files:\n%s",pformat(files, indent=20,
                                                    compact=True     ))
        # get rid of files that are too new
        files = set(filter(check_access, files))
        # transfer files
        transferred = transfer_files(files)
        # update processed list
        processed.update(transferred)
        # pickle (append) set of transferred files
        flb.save_list(transferred)
    time.sleep(PERIOD)

def main():
    logger.info("In directory: %s", os.getcwd())
    logger.info("Backing up directory: %s", PATH_TO_DATA)
    logger.info("Remote save location: %s", PATH_TO_REMOTE)
    flb = FileListBuilder(FILE_LIST)
    processed = flb.get_processed()
    print("Got list of processed files.")
    while True:
        # run program loop
        loop(processed, flb)


if __name__ == "__main__":
    # set up logger instance
    logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s',
                       datefmt='%Y-%m-%d %H:%M:%S', filename=LOGFILE,
                       level=logging.INFO)
    logger = logging.getLogger("autocopy")
    logger.info("======Start operation======")
    print("Starting.")
    main()
