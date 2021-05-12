#!/usr/bin/python3
###############################################################################
# GlacierBackup.py
# ---------------------------
# This is the part of the backup process intended to integrate with a cloud
# storage location. It takes the output of a local backup process - a bunch of
# files in a specified directory trusted as minimal archive files - and batch
# uploads them to (in this case) Amazon S3 Glacier Storage with appropriate
# storage and transfer management to mimimise costs.
#
# This is all fully and completely documented here:
#  https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
###############################################################################
import json
import os
from datetime import datetime
import tarfile
import subprocess
import boto3
from botocore.exceptions import ClientError

#variables that should be overridable from the command-line or some global
#config file
optionsOverrideFile = "~/.glacierclient/glacierbackupoptions.json"
cfg = {
    "backupArchiveLocalPath"    : "/tmp/glacierclient",
    "backupCloudReadyFlagFile"  : "backup_set_complete.flag",
    "DEBUGME"                   : True,
    "INFOMSG"                   : True,
    "localEncryptionKey"        : "AReallySecurePasswordForLocalEncryption",
    "opensslbinary"             : "/usr/bin/openssl",
    "GlacierVault"              :  "AVaultThatExists",
    "VaultSizeLimit"            :  1048576,
    "VaultInventoryFile"        : "~/.glacierclient/last_vault_inventory.json",
    "VaultInventoryCacheFile"   : "~/.glacierclient/vault_inventory_cache.json",
    "GlacierOutstandingJobs"    : "~/.glacierclient/glacier_outstanding_jobs.json",
    "VaultInventoryRequestWindow"  : 86400*7,
    "VaultArchiveMinRetentionDays" : 90
}

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
def loadLastActualInventory(invfile) :
    """ Load the last saved actual inventory file from disk if present """
    actualInventory = {
        "InventoryDate" : 0,
        "ArchiveList": []
    }

    invActualPath = os.path.expanduser(invfile)
    try:
        with open(invActualPath) as cf:
            try:
                actualInventory = json.loads(cf.read())
            except Exception as e:
                infoPrint(f'Could not parse Inventory file {invActualPath} - {e}')
                infoPrint(f'Will use defaults')
    except Exception as e:
        infoPrint(f'Could not load local Inventory Cache file {invActualPath} - {e}')
        infoPrint(f'Will use defaults')

    return actualInventory

###############################################################################
def saveLastActualInventory(actualInventory, invfile) :
    """the inverse of the above"""

    locInvFile = os.path.expanduser(invfile)
    try:
        with open(locInvFile,"w") as lif:
            json.dump(actualInventory,lif)
    except Exception as e:
        print(f'ERROR - Unable to write local inventory copy file {locInvFile} - {e}')
        exit(1)

###############################################################################
def loadInventoryCache(invcachefile) :
    """ Load the locally-maintained inventory cache file from disk if present,
        otherwise initialise as empty """
    inventoryCache = {
        "vaultName" : cfg['GlacierVault'],
        "vaultMaxSize" : cfg['VaultSizeLimit'],
        "vaultContents" : [],
    }

    cacheActualPath = os.path.expanduser(invcachefile)
    try:
        with open(cacheActualPath) as cf:
            try:
                inventoryCache = json.loads(cf.read())
            except Exception as e:
                infoPrint(f'Could not parse local Inventory cache file {cacheActualPath} - {e}')
                infoPrint(f'Will use defaults')
    except Exception as e:
        infoPrint(f'Could not load local Inventory Cache file {cacheActualPath} - {e}')
        infoPrint(f'Will use defaults')

    return inventoryCache

###############################################################################
def saveLocalInventoryCache(inventorycache,invcachefile):
    """ the inverse of the above """
    actualcachefile = os.path.expanduser(invcachefile)
    try:
        with open(actualcachefile, "w") as cf:
            json.dump(inventorycache, cf)
    except Exception as e:
        print(f'ERROR unable to save inventory cache file {actualcachefile} - {e}')
        exit(1)

###############################################################################
def loadOutstandingJobsCache(jobcachefile) :
    """ Load the cache of jobs we know are outstanding, initialise to blank if
        empty """
    jobCache = []
    cacheActualPath = os.path.expanduser(jobcachefile)
    try:
        with open(cacheActualPath) as cf:
            try:
                jobCache = json.loads(cf.read())
            except Exception as e:
                infoPrint(f'Could not parse local Job cache file {cacheActualPath} - {e}')
                infoPrint(f'Will use defaults')
    except Exception as e:
        infoPrint(f'Could not load local Job Cache file {cacheActualPath} - {e}')
        infoPrint(f'Will use defaults')

    return jobCache

###############################################################################
def saveOutstandingJobsCache(jobsCache, jobcachefile) :
    """the inverse of the above"""
    actualcachefile = os.path.expanduser(jobcachefile)
    try:
        with open(actualcachefile, "w") as cf:
            json.dump(jobsCache,cf)
    except Exception as e:
        print(f'ERROR unable to save job cache file {actualcachefile} - {e}')
        exit(1)

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

###############################################################################
def checkOutstandingJobsAndUpdateInventoryIfNeeded(jobCache, inventoryCache, localInventoryFile) :
    """ Go to Amazon and check the status of outstanding jobs (from the cache).
    If any of them have completed, retrieve the results. Update the cache
    with any changes """
    newJobCache = []
    newInventoryCache = inventoryCache
    glacier = boto3.client('glacier')
    for cJob in jobCache :
        try:
            response = glacier.describe_job(vaultName=cJob['vaultID'], jobId=cJob['jobId'])
        except ClientError as e:
            print(f'WARN - Could not retrieve job status for {cJob["jobId"]}, error was {e}')
            newJobCache.append(cJob)
        jType = response["Action"]
        jStatus = response["StatusCode"]
        infoPrint(f'Outstanding Job {cJob["jobId"]} of type {jType} is currently in state {jStatus}')
        if jStatus == "Succeeded" :
            newInventory = retrieveInventoryResults(cJob["jobId"], cJob["vaultID"], localInventoryFile)
            debugPrint(f'retrieved inventory: {newInventory}')
            newInventoryCache = reconcileInventory(inventoryCache, newInventory)
            debugPrint(f'Updated local inventory cache to: {newInventoryCache}')
        elif jStatus == "Failed" :
            print(f'WARN - Inventory Retrieve job {cJob["jobId"]} FAILED - {response}')
        else :
            #Job is still running...
            newJobCache.append(cJob)

    return newJobCache, newInventoryCache

###############################################################################
def retrieveInventoryResults(completedJobID, vaultID, localInvFile) :
    """ For an Inventory-retrieve job that we know has completed, go unto
        Amazon and get the results, storing in the file listed """

    glacier = boto3.client('glacier')
    try:
        response = glacier.get_job_output(vaultName=vaultID, jobId=completedJobID)
        respBody = json.loads(response['body'].read())
    except ClientError as e:
        print(f'ERROR - Unable to retrieve job output for completed job {completedJobID}')
        print(f'ERROR - glacier.get_job_output() returned {e}')
        exit(2)

    saveLastActualInventory(respBody, localInvFile)
    return respBody

###############################################################################
def requestNewInventoryFromAmazon(vaultToInventory) :
    """ Request a new Inventory of the specified Vault from Amazon. Note that
    this is a (literally) expensive operation - many requests cost actual money
    so take care to call this as sparingly as possible. OK I exaggerate but
    you know, time is money """
    jobID = 0

    # Construct job parameters
    job_parms = {'Type': 'inventory-retrieval'}
    # Initiate the job
    glacier = boto3.client('glacier')
    try:
        response = glacier.initiate_job(vaultName=vaultToInventory,
                                        jobParameters=job_parms)
    except ClientError as e:
        print(f'ERROR - Unable to request new Inventory for {vaultToInventory} - {e}')
        exit(2)

    return response["jobId"], vaultToInventory


###############################################################################
def reconcileInventory(inventoryCache, newInventory) :
    """ Reconcile a locally-cached inventory against one retrieved from Amazon,
    with the assumption that Amazon's is true **except* for any files uploaded
    since the LastUpdate time """

    newInventoryCache = {
        "vaultName"     : inventoryCache["vaultName"],
        "vaultMaxSize"  : inventoryCache["vaultMaxSize"],
        "lastActualInventoryTime" : int(datetime.fromisoformat(newInventory["InventoryDate"].replace('Z','+00:00')).timestamp()),
        "vaultContents" : []
    }

    #Everything from the Amazon inventory is gospel truth we should use, copy it in:
    for aArchive in newInventory["ArchiveList"] :
        newInventoryCache["vaultContents"].append( {
            "archiveid"   : aArchive["ArchiveId"],
            "description" : aArchive["ArchiveDescription"],
            "uploadTime"  : int(datetime.fromisoformat(aArchive["CreationDate"].replace('Z','+00:00')).timestamp()),
            "size"        : aArchive["Size"]
        })
    #BUT....anything in the local cache NEWER than the inventory date might yet need
    #updating.
    for cArchive in inventoryCache["vaultContents"] :
        if cArchive['uploadTime'] > newInventoryCache["lastActualInventoryTime"] :
            #This cached file isn't (yet) in the actual Amazon inventory, keep it
            newInventoryCache["vaultContents"].append(cArchive)

    return newInventoryCache

###############################################################################
def calculateVaultSize(inventoryCache) :
    """ Calculate the size of the cache based on the inventory data we've got
        for the archives we've tracked """
    vaultSize = 0
    for archive in inventoryCache["vaultContents"] :
        vaultSize += archive["size"]

    return vaultSize

###############################################################################
def estimateNextBackupSize(inventoryCache) :
    """This is a heuristic. We estimate the next backup size as being 10% bigger
    than that LAST backup taken. So work out what that number actually is"""
    latestBackupDate=0
    lastBackupSize=0
    for archive in inventoryCache["vaultContents"] :
        if archive["uploadTime"] > latestBackupDate :
            latestBackupDate = archive["uploadTime"]
            lastBackupSize = archive["size"]

    if lastBackupSize > 0 :
        return int(lastBackupSize * 1.1)
    else :
        #As a default, assume we need to keep 110MB free:
        return int(110 * 1024 * 1024)

###############################################################################
def createArchiveFileList(directoryToArchive) :
    """Helper to create a suitable filelist of file patterns to include
    in the archive being sent to Amazon"""
    filesToArchive = []
    for path, dirs, files in os.walk(directoryToArchive, topdown=True) :
        for fname in files :
            if fname == cfg['backupCloudReadyFlagFile'] :
                continue
            fqname = os.path.join(path,fname)
            filesToArchive.append({"n" : fname, "p" : fqname})
    return filesToArchive

###############################################################################
def createAndEncryptArchiveBlob(filesToArchive, directoryToUse, encryptionKey) :
    """ Final step prior to uploading a new Archive to Glacier is to coalesce
    all the candidate files into a single archive and encrypt it against the
    local encryption key """

    #Create the Archive as a simple TAR ball of everything in the listed
    #directory
    ArchiveFile="GlacierBackup-" + datetime.now().strftime("%Y%m%d%H%M%S") + ".tar"
    FQArchiveFile = os.path.expanduser(os.path.join(directoryToUse, ArchiveFile))
    #The actual contents is a simple "walk" of the directory as it stands:

    #Create the archive from this list:
    with tarfile.open(FQArchiveFile, "w:") as megaArchive:
        for f in filesToArchive :
            megaArchive.add(f["p"], arcname=f["n"])

    #Encrypt this file if an encryption key is set
    if len(encryptionKey)>0 :
        finalArchive = encryptLocalFile(FQArchiveFile,encryptionKey)
    else :
        finalArchive = FQArchiveFile

    return finalArchive

###############################################################################
def uploadArchiveFileToGlacier(archiveToUpload) :
    #nb: Glacier works on byte-strings so we need to actually stream the file:
    try:
        object_data = open(archiveToUpload,"rb")
    except Exception as e :
        print(f'ERROR - Unable to open {archiveToUpload} for transmission to Glacier: {e}')
        exit(1)

    infoPrint(f'Uploading archive {archiveToUpload} to Glacier')
    glacier = boto3.client('glacier')
    try:
        archive = glacier.upload_archive(vaultName=cfg["GlacierVault"],
                                         archiveDescription=archiveToUpload,
                                         body=object_data)
    except ClientError as e :
        prinf(f'ERROR - Upload of {archiveToUpload} to Glacier failed: {e}')
        exit(2)
    finally :
        object_data.close()

    infoPrint(f'Archive upload complete with ID = {archive["archiveId"]}')
    return archive["archiveId"]

###############################################################################
def backupLocalFilesIfNecessary(inventoryCache) :
    """Wrapper function for the main purpose of this script: Determine whether,
    what and how to backup any configured local files """

    markerFile=os.path.expanduser(os.path.join(cfg['backupArchiveLocalPath'], cfg['backupCloudReadyFlagFile']))
    if os.path.exists(markerFile) :
        infoPrint(f'Local Backup system indicates a complete set ready for backup, proceeding:')
    else :
        infoPrint(f'No marker file found. Nothing to send to the cloud')
        return inventoryCache

    #Determine what files should be in the archive blob to upload:
    fileListToAddToBackup = createArchiveFileList(cfg['backupArchiveLocalPath'])

    #Create the Blob to upload:
    archiveToUpload = createAndEncryptArchiveBlob(fileListToAddToBackup,
                                                  cfg['backupArchiveLocalPath'],
                                                  cfg['localEncryptionKey'])
    #Actually do the actual upload:
    uploadedArchiveID = uploadArchiveFileToGlacier(archiveToUpload)

    #archive is an object with data we need to insert into the inventoryCache
    #but not all of the data is there - we need to get some from the OS:
    inventoryCache["vaultContents"].append( {
        "archiveid"   : uploadedArchiveID,
        "description" : archiveToUpload,
        "uploadTime"  : int(datetime.now().timestamp()),
        "size"        : os.path.getsize(archiveToUpload)
    })

    #And since we must assume if we're here we've successfully uploaded
    #everything, we can purge the local directory of all files ready to
    #start the process all over again:
    os.remove(archiveToUpload)
    os.remove(markerFile)
    #TODO: Purge the files in the tarball too
    for blyatme in fileListToAddToBackup :
        os.remove(blyatme['p'])

    return inventoryCache

###############################################################################
###############################################################################
###############################################################################
if __name__ == '__main__':
    cfg = loadOptions(cfg, optionsOverrideFile)
    inventoryCache = loadInventoryCache(cfg['VaultInventoryCacheFile'])
    jobCache = loadOutstandingJobsCache(cfg['GlacierOutstandingJobs'])
    jobCache, inventoryCache = checkOutstandingJobsAndUpdateInventoryIfNeeded(jobCache, inventoryCache, cfg['VaultInventoryFile'])

    #We should request a new Inventory from Amazon if certain conditions apply:
    timeSinceLastInventory = int(datetime.now().timestamp()) - inventoryCache["lastActualInventoryTime"]
    infoPrint(f'It has been {timeSinceLastInventory} seconds since the last Amazon inventory was taken')
    if len(jobCache) == 0 and timeSinceLastInventory >= cfg['VaultInventoryRequestWindow'] :
        infoPrint(f'Amazon inventory probably stale; requesting a new one')
        jobId,vaultID = requestNewInventoryFromAmazon(cfg['GlacierVault'])
        jobCache.append({ "vaultID" : vaultID, "jobId" : jobId })

    #Determine whether we've got space available in the Vault for the next backup,
    #start pruning if not (and DO NOT back anything up)
    inventoryCache['vaultEstimatedTotalSize'] = calculateVaultSize(inventoryCache)
    inventoryCache['vaultEstimatedSpaceRemaining'] = cfg['VaultSizeLimit'] - inventoryCache['vaultEstimatedTotalSize']
    inventoryCache['nextArchiveEstimatedSize'] = estimateNextBackupSize(inventoryCache)
    infoPrint(f'Vault remaining capacity: {inventoryCache["vaultEstimatedSpaceRemaining"]}')
    if inventoryCache['nextArchiveEstimatedSize'] < inventoryCache['vaultEstimatedSpaceRemaining'] :
        infoPrint(f'Vault has sufficient capacity for next estimated backup size ({inventoryCache["nextArchiveEstimatedSize"]}); no pruning required')
        inventoryCache = backupLocalFilesIfNecessary(inventoryCache)
    else :
        infoPrint(f'Insufficient space for another backup; pruning')
        #TODO : Write pruning function

    saveOutstandingJobsCache(jobCache,cfg['GlacierOutstandingJobs'])
    saveLocalInventoryCache(inventoryCache,cfg['VaultInventoryCacheFile'])
