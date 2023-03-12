# About
more info about service: backblaze.com
# Description
Simple  easy python script which logins to backblaze s3 compatatible bucket and delete files in certain "folder" (actually it just a prefix) in buckets that older than 15(for example) days.
I do not use lifecycle rules in bucket for other reason.
# Requirements
- pip3 install -r requirements.txt
- Create `.env` file with required keys.
