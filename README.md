# About
More info about service: backblaze.com
I need this script because I backed up some files from Synology NAS to Backblaze cloud, but the tool provided by Synology OS does not auto-remove old files.

# Description
Simple  easy Python script that logins to backblaze s3 compatible bucket and deletes files in a certain "folder" (actually it is just a prefix) in buckets that are older than 15(for example) days.
I do not use lifecycle rules in the bucket for other reasons.

# Requirements
- pip3 install -r requirements.txt
- Create `.env` file with the required keys.
