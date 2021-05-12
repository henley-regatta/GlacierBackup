# Glacier Backup Suite
More a sort of proof-of-concept really but a 2-stage backup suite intended for use
on Linux/UNIX systems written in Python that will:
1. Take a regular backup of a set of directories according to specifications
   1. Take a FULL backup at specified intervals
   1. Take INCREMENTAL backups to the given level
   1. Compress the backups to take minimum space
   1. Leave a "Marker" at the end of an Incremental cycle to signal ready for cloud
1. Work with an Amazon S3 Glacier Vault to backup sets of compressed data to that Vault
   1. Triggered by the presence of the "Marker" file
   1. Configured to maintain a local copy of the Vault contents, refreshed periodically
   by requesting new Inventories and doing reconciliation between the two
   1. Will maintain the Vault to a specified size by pruning old backups.
   1. Supports local encryption of the compressed archives in case you don't trust or
   want to trust Amazon's promise of data privacy

Of note: There is at present no "restore" functionality. The intent is that each
Archive in Glacier represents a full and self-contained backup at a point in time (all
be it that it's a `.tar` file containing a Full and a set of Incremental `.tar.bz2` files...), so a retrieve job (coming soon) that got just one of those files could be used to
reconstruct the full backup.

## Usage
1. Follow the instructions in [Preparing the Amazon Environment](https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html#preparing-the-amazon-environment) to setup a Glacier Vault to use
1. Follow the instructions in [Preparing the Python Environment](https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html#preparing-the-python-environment) to load
the necessary AWS modules and setup authentication for the Vault user
1. Create directory `~/.glacierclient`
    1. Create `~/.glacierclient/includeexclude.json` with a list of directories to
    backup and file types / directory names to exclude. There's an [example of the format and contents in the blog documentation](https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html#generating-the-backup-increment---what-should-be-included)
    1. You probably want to create `~/.glacierclient/localbackupoptions.json` to
    override the script defaults for temporary paths, openssl location, and debug levels
    at the very least.
    1. You _almost certainly_ want to create `~/.glacierclient/glacierbackupoptions.json`
    to override the script defaults for Encryption, Vault Name, Max Size and the
    temp paths to match the local backup options
1. The assumption is that the `GlacierBackup.py` script should be called "regularly"
to let it process any outstanding AWS jobs. "Regularly" is a matter of taste - job
output expires 24 hrs after completion plus if your backups are taken daily you'd
like any inventory pruning to occur _prior_ to needing to backup the next full set.
So you might want to run this twice as often as the local backup. In which case,
adding the following lines to your **crontab** (try `crontab -e` to edit) would
work:
```
07 03 * * * /usr/local/bin/LocalIncrementalBackup.py >/tmp/LocalIncrementalBackup.log 2>&1
37 03,15 * * * /usr/local/bin/GlacierBackup.py >>/tmp/GlacierBackup.log 2>&1
```

## Documentation
Full documentation, including the expected AWS S3 Glacier setup, is available for now
[via the associated Blog Post](https://www.guided-naafi.org/systemsmanagement/2021/05/06/WritingMyOwnGlacierBackupClient.html)

## Cautions and Warnings
This suite isn't fully tested and has NO warranty whatsoever. Use at your own risk.
Note in particular that because it interacts with the pay-per-use cloud storage
facilities, you may incurr (real) financial costs in using this set of programs. Don't
say I haven't warned you!
