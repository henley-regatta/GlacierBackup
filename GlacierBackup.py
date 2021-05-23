#!/usr/bin/python3
"""
GlacierBackup.py
----------------

This is the part of the backup process intended to integrate with a cloud
storage location. It takes the output of a local backup process - a bunch of
files in a specified directory trusted as minimal archive files - and batch
uploads them to (in this case) Amazon S3 Glacier Storage with appropriate
storage and transfer management to mimimise costs.
This is all fully and completely documented here:
https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html
"""

import os
from datetime import datetime
import tarfile
import boto3
import json
from botocore.exceptions import ClientError
import BackupSupport #This is own own library of helper functions...


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
    "InventoryRequestMinInterval"  : 86400*2,
    "VaultArchiveMinRetentionDays" : 90
}

###############################################################################
def loadLastActualInventory(invfile, logger) :
    """ Load the last saved actual inventory file from disk if present """
    invActualPath = os.path.expanduser(invfile)
    actualInvData = BackupSupport.loadParseJSONFile(invActualPath,logger)
    if actualInvData is not None :
        return actualInvData
    else :
        logger.infoPrint('Will use dummy Actual Inventory data')
        return {
            "InventoryDate" : 0,
            "ArchiveList": []
        }

###############################################################################
def saveLastActualInventory(actualInventory, invfile, logger) :
    """Save an actual AWS Vault Inventory to local file"""
    locInvFile = os.path.expanduser(invfile)
    BackupSupport.saveDataAsJSONFile(actualInventory,locInvFile, logger)

###############################################################################
def loadInventoryCache(invcachefile,logger) :
    """ Load the locally-maintained inventory cache file from disk if present,
        otherwise initialise as empty """
    cacheActualPath = os.path.expanduser(invcachefile)
    invCache = BackupSupport.loadParseJSONFile(cacheActualPath,logger)
    if invCache is not None :
        return invCache
    else :
        logger.infoPrint(f'Using Defaults for Inventory Cache')
        return {
            "vaultName" : cfg['GlacierVault'],
            "vaultMaxSize" : cfg['VaultSizeLimit'],

            "vaultContents" : [],
        }

###############################################################################
def saveLocalInventoryCache(inventorycache,invcachefile,logger):
    """ Save our cached Inventory data to local file """
    actualcachefile = os.path.expanduser(invcachefile)
    BackupSupport.saveDataAsJSONFile(inventorycache,actualcachefile,logger)

###############################################################################
def loadOutstandingJobsCache(jobcachefile,logger) :
    """ Load the cache of jobs we know are outstanding, initialise to blank if
        empty """
    cacheActualPath = os.path.expanduser(jobcachefile)
    jobCache = BackupSupport.loadParseJSONFile(cacheActualPath,logger)
    if jobCache is not None :
        return jobCache
    else :
        logger.infoPrint(f'Assuming empty Job Cache')
        return []

###############################################################################
def saveOutstandingJobsCache(jobsCache, jobcachefile, logger) :
    """Save the cache of outstanding AWS jobs to local file"""
    actualcachefile = os.path.expanduser(jobcachefile)
    BackupSupport.saveDataAsJSONFile(jobsCache,actualcachefile, logger)

###############################################################################
def checkOutstandingJobsAndUpdateInventoryIfNeeded(jobCache, inventoryCache, localInventoryFile, logger) :
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
            logger.warnPrint(f'Could not retrieve job status for {cJob["jobId"]}, error was {e}')
            newJobCache.append(cJob)
        jType = response["Action"]
        jStatus = response["StatusCode"]
        logger.infoPrint(f'Outstanding Job {cJob["jobId"]} of type {jType} is currently in state {jStatus}')
        if jStatus == "Succeeded" :
            newInventory = retrieveInventoryResults(cJob["jobId"], cJob["vaultID"], localInventoryFile, logger)
            logger.debugPrint(f'retrieved inventory: {newInventory}')
            newInventoryCache = reconcileInventory(inventoryCache, newInventory)
            logger.debugPrint(f'Updated local inventory cache to: {newInventoryCache}')
        elif jStatus == "Failed" :
            logger.warnPrint(f'Inventory Retrieve job {cJob["jobId"]} FAILED - {response}')
        else :
            #Job is still running...
            newJobCache.append(cJob)

    return newJobCache, newInventoryCache

###############################################################################
def retrieveInventoryResults(completedJobID, vaultID, localInvFile, logger) :
    """ For an Inventory-retrieve job that we know has completed, go unto
        Amazon and get the results, storing in the file listed """

    glacier = boto3.client('glacier')
    try:
        response = glacier.get_job_output(vaultName=vaultID, jobId=completedJobID)
        respBody = json.loads(response['body'].read())
    except ClientError as e:
        logger.errorPrint(f'Unable to retrieve job output for completed job {completedJobID}')
        logger.errorPrint(f'glacier.get_job_output() returned {e}')
        exit(2)

    saveLastActualInventory(respBody, localInvFile,logger)
    return respBody

###############################################################################
def requestNewInventoryFromAmazon(vaultToInventory, logger) :
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
        logger.errorPrint(f'Unable to request new Inventory for {vaultToInventory} - {e}')
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
        "lastInventoryReceivedTime" : int(datetime.now().timestamp()),
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
def createAndEncryptArchiveBlob(filesToArchive, directoryToUse, encryptionKey, logger) :
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
        finalArchive = BackupSupport.encryptLocalFile(FQArchiveFile,encryptionKey,cfg['opensslbinary'], logger)
    else :
        finalArchive = FQArchiveFile

    return finalArchive

###############################################################################
def uploadArchiveFileToGlacier(archiveToUpload, logger) :
    """ The actual core of the script. Upload an Archive to an AWS S3 Vault """

    #nb: Glacier works on byte-strings so we need to actually stream the file:
    try:
        object_data = open(archiveToUpload,"rb")
    except Exception as e :
        logger.errorPrint(f'ERROR - Unable to open {archiveToUpload} for transmission to Glacier: {e}')
        exit(1)

    logger.infoPrint(f'Uploading archive {archiveToUpload} to Glacier')
    glacier = boto3.client('glacier')
    try:
        archive = glacier.upload_archive(vaultName=cfg["GlacierVault"],
                                         archiveDescription=archiveToUpload,
                                         body=object_data)
    except ClientError as e :
        logger.errorPrint(f'Upload of {archiveToUpload} to Glacier failed: {e}')
        exit(2)
    finally :
        object_data.close()

    logger.infoPrint(f'Archive upload complete with ID = {archive["archiveId"]}')
    return archive["archiveId"]

###############################################################################
def backupLocalFilesIfNecessary(inventoryCache,logger) :
    """Wrapper function for the main purpose of this script: Determine whether,
    what and how to backup any configured local files """

    markerFile=os.path.expanduser(os.path.join(cfg['backupArchiveLocalPath'], cfg['backupCloudReadyFlagFile']))
    if os.path.exists(markerFile) :
        logger.infoPrint(f'Local Backup system indicates a complete set ready for backup, proceeding:')
    else :
        logger.infoPrint(f'No marker file found. Nothing to send to the cloud')
        return inventoryCache

    #Determine what files should be in the archive blob to upload:
    fileListToAddToBackup = createArchiveFileList(cfg['backupArchiveLocalPath'])

    #Create the Blob to upload:
    archiveToUpload = createAndEncryptArchiveBlob(fileListToAddToBackup,
                                                  cfg['backupArchiveLocalPath'],
                                                  cfg['localEncryptionKey'],
                                                  logger)
    #Actually do the actual upload:
    uploadedArchiveID = uploadArchiveFileToGlacier(archiveToUpload, logger)

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
def pruneVaultToSpecifiedFreeSpace(inventoryCache, requiredExtraSpace, logger) :
    """ Attempt to prune the Vault to free up space to enable next backup to be
        taken. Uses aging as well as size heuristics """
    minAge = cfg['VaultArchiveMinRetentionDays'] * 86400
    now = datetime.now().timestamp()
    candidateArchives = {} #Index by age, better to sort by

    #This is going to make the bookkeping a bit easier at the end
    currentArchives = {}
    for arc in inventoryCache['vaultContents'] :
        currentArchives[arc['archiveid']] = arc

    # Look through the inventory for all archives OLDER than the age threshold:
    for (aid, arc) in currentArchives.items() :
        arcAge = now - arc['uploadTime']
        logger.debugPrint(f'PRUNING: {arc["description"]} is {arcAge/86400} days old')
        if arcAge > minAge :
            logger.debugPrint(f'PRUNING {arc["description"]} can be pruned')

    #Now thin this out to the OLDEST archives that SUM to match the required space:
    pruneArchives=[]
    pruneSpace=0
    for d in sorted(candidateArchives) :
        if pruneSpace > requiredExtraSpace :
            #We have enough space to prune, don't add any more
            break
        else :
            pruneSpace += candidateArchives[d]['size']
            pruneArchives.append(candidateArchives[d])
    logger.debugPrint(f'PRUNING: Will Prune {pruneArchives} to free up {pruneSpace} bytes')

    #Go through and run the deletions on them:
    spaceToGo = requiredExtraSpace
    for delet_dis in pruneArchives :
        logger.infoPrint(f'PRUNING: Deleting {delet_dis["archiveid"]} to free {delet_dis["size"]}')
        isGone = pruneArchive(cfg['GlacierVault'], delet_dis['archiveid'],logger)
        if isGone :
            #Remove the archive from the inventory,
            spaceToGo -= delet_dis['size']
            del currentArchives[delet_dis['archiveid']]
        else :
            logger.warnPrint(f'pruneArchive failed for {delet_dis["archiveid"]}')

    #Update the inventoryCache based on what's left
    inventoryCache['vaultContents'] = list(currentArchives.values())

    return inventoryCache, spaceToGo<0


###############################################################################
def pruneArchive(vault_name,archive_id,logger) :
    """ Issue request to AWS to actually delete an archive. Operation is
    synchronous so return value can be used to evaluate success/failure """

    glacier = boto3.client('glacier')
    try:
        response = glacier.delete_archive(vaultName=vault_name,
                                          archiveId=archive_id)
    except ClientError as e:
        logger.warnPrint(f'glacier.delete_archive failed {e}')
        return False
    return True

###############################################################################
###############################################################################
###############################################################################
if __name__ == '__main__':
    #Initialise logging support
    logger = BackupSupport.BSLogHelper('GlacierBackup',cfg['DEBUGME'],cfg['INFOMSG'])

    #Load options from backup file
    cfg = BackupSupport.loadOptions(cfg, optionsOverrideFile, logger)

    #Re-set log level based on changed config
    logger.setLogLevel(cfg['DEBUGME'], cfg['INFOMSG'])

    inventoryCache = loadInventoryCache(cfg['VaultInventoryCacheFile'],logger)
    jobCache = loadOutstandingJobsCache(cfg['GlacierOutstandingJobs'],logger)
    jobCache, inventoryCache = checkOutstandingJobsAndUpdateInventoryIfNeeded(jobCache, inventoryCache, cfg['VaultInventoryFile'],logger)

    #We should request a new Inventory from Amazon if certain conditions apply:
    timeSinceLastInventory = int(datetime.now().timestamp()) - inventoryCache["lastActualInventoryTime"]
    timeSinceLastInventoryRequest = int(datetime.now().timestamp()) - inventoryCache["lastInventoryReceivedTime"]
    logger.infoPrint(f'It has been {timeSinceLastInventory} seconds since the last Amazon inventory was taken')
    logger.infoPrint(f'and it has been {timeSinceLastInventoryRequest} seconds since we last requested one from Amazon')
    if ( len(jobCache) == 0 and
         timeSinceLastInventory >= cfg['VaultInventoryRequestWindow'] and
         timeSinceLastInventoryRequest > cfg['InventoryRequestMinInterval']
       ):
        logger.infoPrint(f'Amazon inventory probably stale; requesting a new one')
        jobId,vaultID = requestNewInventoryFromAmazon(cfg['GlacierVault'],logger)
        jobCache.append({ "vaultID" : vaultID, "jobId" : jobId })

    #Determine whether we've got space available in the Vault for the next backup,
    #start pruning if not (and DO NOT back anything up)
    inventoryCache['vaultEstimatedTotalSize'] = calculateVaultSize(inventoryCache)
    inventoryCache['vaultEstimatedSpaceRemaining'] = cfg['VaultSizeLimit'] - inventoryCache['vaultEstimatedTotalSize']
    inventoryCache['nextArchiveEstimatedSize'] = estimateNextBackupSize(inventoryCache)
    logger.infoPrint(f'Vault remaining capacity: {inventoryCache["vaultEstimatedSpaceRemaining"]}')
    if inventoryCache['nextArchiveEstimatedSize'] < inventoryCache['vaultEstimatedSpaceRemaining'] :
        logger.infoPrint(f'Vault has sufficient capacity for next estimated backup size ({inventoryCache["nextArchiveEstimatedSize"]}); no pruning required')
        inventoryCache = backupLocalFilesIfNecessary(inventoryCache, logger)
    else :
        requiredSpaceToPrune = inventoryCache['nextArchiveEstimatedSize'] - inventoryCache['vaultEstimatedSpaceRemaining']
        logger.infoPrint(f'Insufficient space for another backup; need {requiredSpaceToPrune} bytes. Pruning...')
        inventoryCache, freedUpEnoughSpace = pruneVaultToSpecifiedFreeSpace(inventoryCache, requiredSpaceToPrune, logger)
        if freedUpEnoughSpace :
            logger.infoPrint(f'Pruning cleared enough space; running backup')
            inventoryCache = backupLocalFilesIfNecessary(inventoryCache, logger)
        else :
            logger.warnPrint(f'Pruning Vault Space did not free up enough space. Cloud backups inhibited')

    saveOutstandingJobsCache(jobCache,cfg['GlacierOutstandingJobs'],logger)
    saveLocalInventoryCache(inventoryCache,cfg['VaultInventoryCacheFile'],logger)
