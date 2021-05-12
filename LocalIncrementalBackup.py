#!/usr/bin/python3
###############################################################################
# LocalIncrementalBackup.py
# ---------------------------
# Generate a set of local incremental backups in a given output/temp directory.
# Intended as "Stage 1" of a 2 stage process - stage 2 will process the files
# created and shovel them off safely / securely to some off-board possibly
# cloudy location...
#
# This is all fully and completely documented here:
#  https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
###############################################################################
import json
import os
import hashlib
from datetime import datetime
import tarfile
import subprocess
import glob

#variables that should be overridable from the command-line or some global
#config file
optionsOverrideFile = "~/.glacierclient/localbackupoptions.json"
cfg = {
    "includeexcludefilespec" : "~/.glacierclient/includeexclude.json",
    "previousFileStateStore" : "~/.glacierclient/previousfilestore.json",
    "backupArchiveLocalPath" : "/tmp/glacierclient",
    "opensslbinary"          : "/usr/bin/openssl",
    "maxIncrementsBetweenFullBackups" : 7,
    "localEncryptionKey" : "",
    "DEBUGME" : True,
    "INFOMSG" : True
}

###############################################################################
#Globals we'll build and manipulate
currentFileHashes = {}
forbiddenFileExtensions = []
excludeList = set()

###############################################################################
def debugPrint(msg) :
    global cfg
    if cfg['DEBUGME'] :
        print(f'DEBUG {msg}')

###############################################################################
def infoPrint(msg) :
    global cfg
    if cfg['INFOMSG'] :
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
def getFileSpecs(fileSpec) :
    """ Load the include / exclude specifications from external JSON, with a
    certain amount of error handling """
    incexcspecactualpath = os.path.expanduser(fileSpec)
    try:
        with open(incexcspecactualpath) as sf:
            try:
                fspecs = json.loads(sf.read())
                return fspecs
            except Exception as e:
                print(f'ERROR failure parsing JSON in {incexcspecactualpath} : {e}')
                exit(2)
    except Exception as e:
        print(f'ERROR reading include/exclude file {incexcspecactualpath} : {e}')
        exit(1)

###############################################################################
def loadPreviousBackupData(fileSpec) :
    """ Load the stored previous backup state, if available """
    prevStateactualPath = os.path.expanduser(fileSpec)
    prevState = { 'metadata' : {'lastBackupTS': 0 }}
    try:
        with open(prevStateactualPath) as psf:
            try:
                prevState = json.loads(psf.read())
            except Exception as e:
                infoPrint(f'Could not parse JSON from {prevStateactualPath}, will assume fresh backup required (error: {e})')
    except Exception as e:
        infoPrint(f'Could not open file {prevStateactualPath}, will assume fresh backup required (error: {e})')
    return prevState

###############################################################################
def writeNewBackupData(fileSpec, newMetaData, newFileHashList) :
    oldStateActualPath = os.path.expanduser(fileSpec)
    try:
        with open(oldStateActualPath, 'w') as csf:
            json.dump({"metadata" : newMetaData, "filelist" : newFileHashList}, csf)
    except Exception as e:
        print(f'ERROR: Unable to write backup state file {oldStateActualPath} because {e}')

###############################################################################
#Optimisations used by the individual file parser for exceptions below:
def prepareExclusionLists(exclusions) :
    """ Build the global forbiddenFileExtensions list """
    global excludeList
    global forbiddenFileExtensions
    exDirs = []
    for exCand in exclusions:
        if exCand[0] == "*" :
            forbiddenFileExtensions.append(exCand[1:])
        else :
            exDirs.append(exCand)
    # Similar but used by the directory walker to exclude matching directories:
    excludeList = set(exDirs)
    infoPrint(f'excluding files with extensions {forbiddenFileExtensions}')
    infoPrint(f'excluding directory trees from: {excludeList}')

###############################################################################
def matchFileExtension(filename) :
    """ Checks the file extension (...ok, end of file) to see if it matches
    any of the listed forbidden file extensions """
    for forbidden in forbiddenFileExtensions:
        if filename.endswith(forbidden) :
            return True
    #If we got here it's OK
    return False

###############################################################################
def getFileHash(fqfilename)  :
    """Returns the secure hash of a passed filename if it can be read, 0 if not """
    hash_blake2b = hashlib.blake2b()
    try:
        with open(fqfilename,"rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_blake2b.update(chunk)
        return hash_blake2b.hexdigest()
    except Exception as e:
        print(f'WARN Error generating hash for {fqfilename}: {e}')
        return 0

###############################################################################
def buildCurrentFileHashes(pathsToCheck) :
    """ Iterate down over every requested file spec and build the list of matching, valid
    files with their hashes into the current hashes list """
    allFileHashes = {}
    for filespec in pathsToCheck :
        infoPrint(f'Scanning spec: {filespec}')
        for path, dirs, files in os.walk(filespec, topdown=True) :
            #Map out any dirs matching the excludes list
            dirs[:] = [d for d in dirs if d not in excludeList]
            debugPrint(f'Scanning path: {path}')
            for fname in files:
                if not matchFileExtension(fname) :
                    fqname = os.path.join(path, fname)
                    allFileHashes[fqname] = getFileHash(fqname)
    return allFileHashes

###############################################################################
def buildfileListToBackup(current,previous) :
    """ Build the list of files to include in the current backup based on the
    heuristics we've specified like incremental periodicity, changes etc """
    backupFileList = []
    global cfg
    #Special-case processing first.
    # A) Full backup if no previous metadata or file hash exists
    if "metadata" not in previous or "filelist" not in previous :
        backupFileList = list(current.keys())
        return backupFileList
    prevMeta = previous["metadata"]
    # B) Full backup if no history
    if "lastBackupTS" not in prevMeta or prevMeta["lastBackupTS"] == 0 :
        backupFileList = list(current.keys())
        return backupFileList
    # C) Full backup if no list of incrementals OR number of incrementals
    #    exceeds the limit defined above
    if "numIncrementals" not in prevMeta or prevMeta["numIncrementals"] > cfg['maxIncrementsBetweenFullBackups'] :
        backupFileList = list(current.keys())
        return backupFileList
    # D) Full backup if the archive directory is empty (because we need a base
    #    file on which to do backups...
    full_backs = glob.glob(cfg['backupArchiveLocalPath'] + "/*_full.tar*")
    if len(full_backs) == 0 :
        cfg['OverrideTakeFullBackup'] = True
        backupFileList = list(current.keys())
        return backupFileList

    #OK General case processing. Loop through all the files in the current list.
    #convenience:
    prevFiles = previous["filelist"]
    for cFile, cHash in current.items() :
        #If it doesn't exist in the previous list, auto-backup:
        if cFile not in prevFiles :
            backupFileList.append(cFile)
        #Otherwise if the hashes don't match, back up:
        elif cHash != prevFiles[cFile] :
            backupFileList.append(cFile)

    return backupFileList

###############################################################################
def generateNewMetadata(previousMetaData) :
    """Generate updated metadata to store based on the current set of files and
    the history available"""
    cBackupTS = datetime.now().strftime("%Y%m%d%H%M%S")
    #remaining values depend on whether last backup was full/incremental and
    #whether this backup is/should be incremental:
    if "numIncrementals" in previousMetaData and previousMetaData["numIncrementals"] == cfg['maxIncrementsBetweenFullBackups'] :
        # MCE 2021-05-10 - Want to signal to external cloud program that the "set"
        # of files from the previous backup is ready for upload, because this is
        # the last incremental to be created:
        cloudBackupReadyFlagFile = os.path.expanduser(os.path.join(cfg['backupArchiveLocalPath'], "backup_set_complete.flag"))
        try:
            with open(cloudBackupReadyFlagFile, "w") as ff:
                ff.write(cBackupTS)
            infoPrint(f'Since this was the last incremental backup, wrote flag-file {cloudBackupReadyFlagFile} to signal ready for cloud Backup')
        except Exception as e:
            print(f'WARN Unable to create flag file for cloud backup as {cloudBackupReadyFlagFile}, error {e}')
    if "OverrideTakeFullBackup" in cfg or "numIncrementals" not in previousMetaData or previousMetaData["numIncrementals"] > cfg['maxIncrementsBetweenFullBackups'] :
        cIncrementals = 0
        cArchiveName = cBackupTS + "_full"

    else :
        cIncrementals = previousMetaData["numIncrementals"] + 1
        infoPrint(f'This will be an Incremental backup {cIncrementals} of {cfg["maxIncrementsBetweenFullBackups"]}')
        cArchiveName = cBackupTS + "_incr_from_" + previousMetaData["lastBackupTS"]

    #Build and return the new metadata struct:
    return { "lastBackupTS" : cBackupTS,
             "numIncrementals" : cIncrementals,
             "archiveName" : cArchiveName}

###############################################################################
def createLocalArchive(filename, filelist):
    """Create the local compressed archive file from the generated name and
    file list to be backed up. Returns the actual filename created"""
    archiveFile = filename + ".tar.bz2"
    with tarfile.open(archiveFile, "w:bz2") as arc:
        for src in filelist:
            arc.add(src)
    return archiveFile

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
        print(f' ERRROR encrypting archive, returned {e}')
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

###############################################################################
def runLocalBackup(cfg) :
    """A wrapper for Part One of the requirement - create a local backup archive.
      There's nothing to stop you using this stand-alone as the result is a chain
      of local archive files in the specified tmp dir consisting of sets of full
      followed by incremental backup files"""

    fspecs = getFileSpecs(cfg['includeexcludefilespec'])
    prepareExclusionLists(fspecs['excludes'])
    currentFileHashes = buildCurrentFileHashes(fspecs['includes'])
    previousBackupData = loadPreviousBackupData(cfg['previousFileStateStore'])
    thisBackupFileList = buildfileListToBackup(currentFileHashes,previousBackupData)
    infoPrint(f'Current backup set contains {len(thisBackupFileList)} files out of {len(currentFileHashes.keys())} found by scan')
    currentMetaData = generateNewMetadata(previousBackupData["metadata"])
    localBackupFile = os.path.join(cfg['backupArchiveLocalPath'],currentMetaData["archiveName"])
    backupFileName = createLocalArchive(localBackupFile,thisBackupFileList)
    if "localEncryptionKey" in cfg and len(cfg['localEncryptionKey'])>0 :
        backupFileName = encryptLocalFile(backupFileName,cfg['localEncryptionKey'])
    infoPrint(f'Created local archive file in {backupFileName}')
    writeNewBackupData(cfg['previousFileStateStore'], currentMetaData, currentFileHashes)
    return backupFileName

###############################################################################
###############################################################################
###############################################################################
if __name__ == '__main__':
    cfg = loadOptions(cfg, optionsOverrideFile)
    localBackupFile = runLocalBackup(cfg)