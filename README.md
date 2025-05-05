These files are raw production versions uploaded to allow for backup personnel to get used to the code.  
They're already being superceeded by newer versions because of feature and functionality changes.

You should be able to run them using the base conda environment on `ttru-aware.wwbp.org`  
`python <filename>`   
should be fine.

In order to run them, you'll also need 3 confg files (kept separately because they contain passwords and keys)  
You'll need a .my.cnf file in your home directory with a login that has read access to the db and tables  
You'll need a .my.qualtrics.cnf in your home directory that has all of our qualtrics tokens in it  (The stub file here just contains markers)  
You'll need a .smart-r-config.json file that has the google project info in it (The stub file here is just an example)  

Because these files weren't meant to be saved off at this point, they still have a few hardcoded /home/douglasvbellew/ directores in there.  
You'll need to fix those.  I'll make it better in future versions.  
# PRODUCTION:
The files are being run in production via cron job (sudo crontab -e)

Here are the current cron jobs:
```
# REMEMBER - SERVER TIMEZONE IS UTC SO MODIFY TIMES ACCORDINGLY 
# m h  dom mon dow   command
30 13 * * * su douglasvbellew -c "/usr/lib/anaconda3/bin/python /home/douglasvbellew/Analyze_AWARE_User_Data_Daily_Prod.py"
30 15 * * * su douglasvbellew -c "/usr/lib/anaconda3/bin/python /home/douglasvbellew/Reverse_Geocode_Users_Prod.py"
30 15 * * * su douglasvbellew -c "/usr/lib/anaconda3/bin/python /home/douglasvbellew/Get_SMS_Topic_Cos_Similarity_Top_10_Try_2_1_Prod.py"
```
