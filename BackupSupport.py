###############################################################################
# BackupSupport.py
# ---------------------------
# Common functions used by the Local and Glacier backup programs.
# A library, if you will.
#
# This is all fully and completely documented here:
#  https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
###############################################################################
import json
import os
from datetime import datetime
import subprocess

###############################################################################
def debugPrint(msg,doPrint) :
    if doPrint :
        print(f'DEBUG {msg}')

###############################################################################
def infoPrint(msg,doPrint) :
    if doPrint :
        print(f'INFO {msg}')

###############################################################################
def loadOptions(currentCfg, optionsfile) :
    """Load the external options file to override hard-coded values if required"""
    actualOptionsFilePath = os.path.expanduser(optionsfile)
    try:
        with open(actualOptionsFilePath) as of:
            try:
                newCfg = json.loads(of.read())
            except Exception as e:
                infoPrint(f'Could not parse options file {actualOptionsFilePath} will use defaults ({e})')
                return currentCfg
    except Exception as e:
        infoPrint(f'Could not open options file {actualOptionsFilePath} will use defaults ({e})')
        return currentCfg

    combinedCfg = {}
    for k in currentCfg.keys() :
        if k in newCfg :
            combinedCfg[k] = newCfg[k]
        else :
            combinedCfg[k] = currentCfg[k]

    return combinedCfg

###############################################################################
def encryptLocalFile(filename, password):
    """Encrypt a local file using openssl with a password. Not the strongest
    or safest but better than nothing and, crucially, won't require this script
    to decrypt later, just openssl"""
    encryptedFileName = filename + ".enc"

    try:
        subprocess.run([cfg['opensslbinary'], 'enc', '-aes-256-cbc', '-pbkdf2',
                        '-salt', '-in', filename, '-out', encryptedFileName,
                        '-k', password], capture_output=False, shell=False)
    except Exception as e:
        print(f'ERROR encrypting archive, returned {e}')
        exit(3)

    #Check the archive got created OK
    if os.path.exists(encryptedFileName) and os.path.getsize(encryptedFileName) > 0 :
        #It's there. Remove the original file, return pointer to encrypted
        try:
            os.remove(filename)
        except Exception as e:
            print(f' WARN could not remove original archive {filename} : {e}')
        return encryptedFileName
    else :
        print(f' ERROR Encrypted file {encryptedFileName} invalid after ssl. Something Went Wrong')
        exit(4)
