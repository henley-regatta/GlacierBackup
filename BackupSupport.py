"""
BackupSupport.py
----------------

Common functions used by the Local and Glacier backup programs.
A library, if you will.

This is all fully and completely documented here:
https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
"""
import json
import os
from datetime import datetime
import subprocess

###############################################################################
def tsStr() :
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

###############################################################################
def debugPrint(msg,doPrint) :
    """Print a formatted DEBUG message if enabled"""
    if doPrint :
        print(f'{tsStr()} DEBUG {msg}')

###############################################################################
def infoPrint(msg,doPrint) :
    """Print a formatted INFO message if enabled"""
    if doPrint :
        print(f'{tsStr()} INFO {msg}')

###############################################################################
def warnPrint(msg) :
    """(always) print a formatted warning message"""
    print(f'{tsStr()} WARN {msg}')

###############################################################################
def errorPrint(msg) :
    """(always) print a formatted error message"""
    print(f'{tsStr()} ERROR {msg}')

###############################################################################
def saveDataAsJSONFile(dataStructure,path_to_json_file) :
    """Write a data structure as JSON to a file, abort on failure"""
    try:
        with open(path_to_json_file, "w") as wf:
            json.dump(dataStructure, wf)
    except Exception as e:
        errorPrint(f'Unable to save data to JSON file {path_to_json_file} - {e}')
        exit(1)

###############################################################################
def loadParseJSONFile(path_to_json_file,printErrors) :
    """Load a data structure from JSON stored in an external file. Returns
       either the data structure or None if there's an error"""
    try:
        with open(path_to_json_file) as jf:
            try:
                readInJSON = json.loads(jf.read())
            except Exception as e:
                warnPrint(f'Could not parse JSON from file {path_to_json_file}, {e}')
                return None
    except Exception as e:
        infoPrint(f'Could not read file {path_to_json_file}, {e}',printErrors)
        return None

    return readInJSON

###############################################################################
def loadOptions(currentCfg, optionsfile, printErrors) :
    """Load the external options file to override hard-coded values if required"""
    actualOptionsFilePath = os.path.expanduser(optionsfile)
    newCfg = loadParseJSONFile(actualOptionsFilePath,printErrors)
    if newCfg is not None:
        combinedCfg = {}
        for k in currentCfg.keys() :
            if k in newCfg :
                combinedCfg[k] = newCfg[k]
            else :
                combinedCfg[k] = currentCfg[k]
        return combinedCfg

    infoPrint(f'No configuration overrides available, using default',printErrors)
    return currentCfg

###############################################################################
def encryptLocalFile(filename, password, sslBinaryLocation):
    """Encrypt a local file using openssl with a password. Not the strongest
    or safest but better than nothing and, crucially, won't require this script
    to decrypt later, just openssl"""
    encryptedFileName = filename + ".enc"

    try:
        subprocess.run([sslBinaryLocation, 'enc', '-aes-256-cbc', '-pbkdf2',
                        '-salt', '-in', filename, '-out', encryptedFileName,
                        '-k', password], capture_output=False, shell=False)
    except Exception as e:
        errorPrint(f'Failed to encrypt archive, returned {e}')
        exit(3)

    #Check the archive got created OK
    if os.path.exists(encryptedFileName) and os.path.getsize(encryptedFileName) > 0 :
        #It's there. Remove the original file, return pointer to encrypted
        try:
            os.remove(filename)
        except Exception as e:
            warnPrint(f'Could not remove original archive {filename} : {e}')
        return encryptedFileName
    else :
        errorPrint(f'Encrypted file {encryptedFileName} invalid after ssl. Something Went Wrong')
        exit(4)
