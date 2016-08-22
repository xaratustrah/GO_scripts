Autocopy
========

This code is written by @jepio.

Autocopy is a python script that periodically transfers data from a local folder to a remote location. It's primary design goal is moving files from Windows machines to Unix ones.

Deployment
-----------

1.  Right click and run the attached powershell script `getstuff.ps1` which will download the neccessary putty components as well as python.
    * if this does not work *ootb* launch a powershell instance with `powershell.exe -ExecutionPolicy Unrestricted`.
    * if you can't find the files they will probably be in the current users home directory (C:\Users\User), sorry.
2.  Launch **puttygen.exe**, generate a key pair, save the private key somehwere, copy the public key in the grey text box into your clipboard (OpenSSH public key).
3.  Log into your remote using **putty.exe** and append the OpenSSH key to `.ssh/authorized_keys` (e.g. by doing `echo "<right mouse click>" >> .ssh/authorized_keys`)
4.  Launch **pagent.exe** on your local machine and import the private key. You will now be able to log into your remote with putty without a password (and perform secure copies with no password too).
5.  Edit the **autocopy.py** file to suit your needs. You must check the global variables at the beginning to see if they are what you expect. Most importantly:
    * set PERIOD to how often in seconds you want to perform backups.
    * set HOST to "user@host", the remote host location.
    * set REMOTE_FOLDER to the folder to which you want to copy. It must exist, and it should be a path relative to the users     home folder. The `posixpath.join` function should be used with longer paths.
    * set PATH_TO_DATA to the desired path, using `os.path.join`. This is the folder that will be monitored.
6.  Launch **autocopy.py** and observe the log file to see what's happening.
    

Warning
--------

If you previously used putty and have set it to keep a log, be careful because pscp will also follow this setting and if you chose full ssh packet log the log size may get out of hand. 

Other than that, enjoy!
