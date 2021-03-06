#!/usr/bin/python3
"""
LocalIncrementalBackup.py
-------------------------
Generate a set of local incremental backups in a given output/temp directory.
Intended as "Stage 1" of a 2 stage process - stage 2 will process the files
created and shovel them off safely / securely to some off-board possibly
cloudy location...

This is all fully and completely documented here:
https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
"""

import os
import hashlib
from datetime import datetime
import tarfile
import glob
import BackupSupport #This is own own library of helper functions...

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
def getFileSpecs(fileSpec, logger) :
    """ Load the include / exclude specifications from external JSON, with a
    certain amount of error handling """
    incexcspecactualpath = os.path.expanduser(fileSpec)
    fspecs = BackupSupport.loadParseJSONFile(incexcspecactualpath,logger)
    if fspecs is not None :
        return fspecs
    else :
        #Can't go on without a file specification to load, abort
        logger.errorPrint(f'Cannot proceed without a file specification to backup')
        exit(1)

###############################################################################
def loadPreviousBackupData(fileSpec, logger) :
    """ Load the stored previous backup state, if available """
    prevStateactualPath = os.path.expanduser(fileSpec)
    prevState = BackupSupport.loadParseJSONFile(prevStateactualPath,logger)
    if prevState is not None :
        return prevState
    else :
        logger.infoPrint('Using blank previous backup state')
        return { 'metadata' : {'lastBackupTS': 0 }}

###############################################################################
def writeNewBackupData(fileSpec, newMetaData, newFileHashList, logger) :
    """ Write the state of what files we know about to local file for next time """
    oldStateActualPath = os.path.expanduser(fileSpec)
    backupData = {"metadata" : newMetaData, "filelist" : newFileHashList}
    BackupSupport.saveDataAsJSONFile(backupData,oldStateActualPath, logger)

###############################################################################
#Optimisations used by the individual file parser for exceptions below:
def prepareExclusionLists(exclusions, logger) :
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
    logger.infoPrint(f'excluding files with extensions {forbiddenFileExtensions}')
    logger.infoPrint(f'excluding directory trees from: {excludeList}')

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
def getFileHash(fqfilename, logger) :
    """Returns the secure hash of a passed filename if it can be read, 0 if not """
    hash_blake2b = hashlib.blake2b()
    try:
        with open(fqfilename,"rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_blake2b.update(chunk)
        return hash_blake2b.hexdigest()
    except Exception as e:
        logger.warnPrint(f'Error generating hash for {fqfilename}: {e}')
        return 0

###############################################################################
def buildCurrentFileHashes(pathsToCheck, logger) :
    """ Iterate down over every requested file spec and build the list of matching, valid
    files with their hashes into the current hashes list """
    allFileHashes = {}
    for filespec in pathsToCheck :
        logger.infoPrint(f'Scanning spec: {filespec}')
        for path, dirs, files in os.walk(filespec, topdown=True) :
            #Map out any dirs matching the excludes list
            dirs[:] = [d for d in dirs if d not in excludeList]
            logger.debugPrint(f'Scanning path: {path}')
            for fname in files:
                if not matchFileExtension(fname) :
                    fqname = os.path.join(path, fname)
                    allFileHashes[fqname] = getFileHash(fqname,logger)
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
def generateNewMetadata(previousMetaData, logger) :
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
            logger.infoPrint(f'Since this was the last incremental backup, wrote flag-file {cloudBackupReadyFlagFile} to signal ready for cloud Backup')
        except Exception as e:
            logger.warnPrint(f'Unable to create flag file for cloud backup as {cloudBackupReadyFlagFile}, error {e}')
    if "OverrideTakeFullBackup" in cfg or "numIncrementals" not in previousMetaData or previousMetaData["numIncrementals"] > cfg['maxIncrementsBetweenFullBackups'] :
        cIncrementals = 0
        cArchiveName = cBackupTS + "_full"

    else :
        cIncrementals = previousMetaData["numIncrementals"] + 1
        logger.infoPrint(f'This will be an Incremental backup {cIncrementals} of {cfg["maxIncrementsBetweenFullBackups"]}')
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
def runLocalBackup(cfg,logger) :
    """A wrapper for Part One of the requirement - create a local backup archive.
      There's nothing to stop you using this stand-alone as the result is a chain
      of local archive files in the specified tmp dir consisting of sets of full
      followed by incremental backup files"""

    fspecs = getFileSpecs(cfg['includeexcludefilespec'], logger)
    prepareExclusionLists(fspecs['excludes'], logger)
    currentFileHashes = buildCurrentFileHashes(fspecs['includes'], logger)
    previousBackupData = loadPreviousBackupData(cfg['previousFileStateStore'], logger)
    thisBackupFileList = buildfileListToBackup(currentFileHashes,previousBackupData)
    logger.infoPrint(f'Current backup set contains {len(thisBackupFileList)} files out of {len(currentFileHashes.keys())} found by scan')
    currentMetaData = generateNewMetadata(previousBackupData["metadata"], logger)
    localBackupFile = os.path.join(cfg['backupArchiveLocalPath'],currentMetaData["archiveName"])
    backupFileName = createLocalArchive(localBackupFile,thisBackupFileList)
    if "localEncryptionKey" in cfg and len(cfg['localEncryptionKey'])>0 :
        backupFileName = BackupSupport.encryptLocalFile(backupFileName,cfg['localEncryptionKey'],cfg['opensslbinary'], logger)
    logger.infoPrint(f'Created local archive file in {backupFileName}')
    writeNewBackupData(cfg['previousFileStateStore'], currentMetaData, currentFileHashes, logger)
    return backupFileName

###############################################################################
###############################################################################
###############################################################################
if __name__ == '__main__':

    #Initialise logging support
    logger = BackupSupport.BSLogHelper('LocalIncrementalBackup',cfg['DEBUGME'],cfg['INFOMSG'])

    #Load / override options from file
    cfg = BackupSupport.loadOptions(cfg, optionsOverrideFile, logger)

    #Re-set log level based on changed config
    logger.setLogLevel(cfg['DEBUGME'], cfg['INFOMSG'])

    #And run the backup
    localBackupFile = runLocalBackup(cfg,logger)
