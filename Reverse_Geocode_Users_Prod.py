import sys
import os
import datetime as dt
import getopt
from pathlib import Path
import getpass
import mysql.connector
import configparser
import sqlalchemy as db
import numpy as np
import pandas as pd
import mariadb
from datetime import datetime, timedelta, date, time
import zoneinfo
import dateutil.relativedelta
import tzdata
import requests
import json
import zipfile
import io
import csv
import copy
import smtplib
import glob
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import reverse_geocode

def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", [])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    # SET RUN DEFAULTS
    options = {}
    options["testing"] = False
    options["days_to_get"] = 6
    options["start_day_offset"] = 1 
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["myqualtricsconfig"] = str(str(Path.home()) + '/.my.qualtrics.cnf')
    options["locations_db"] = "OAUTH_7"
    options["locations_table"] = "locations"
    options["report_db"] = "REPORTING"
    options["location_report_table"] = "locations_report"
    options["google_oauth_credentials_file"] = "/home/douglasvbellew/.smart-r-config.json"
    options["google_oauth_scope_directory"] = ["https://www.googleapis.com/auth/drive"]

    if options["testing"]:
        options["csvDirectory"] = "/home/douglasvbellew/loc_test/" # include trailing "/"
        options["csv_Baseline_Directory"] = "/home/douglasvbellew/logs/" # include trailing "/"
    else:
        options["csvDirectory"] = "/home/douglasvbellew/loc_logs/" # include trailing "/"
        options["csv_Baseline_Directory"] = "/home/douglasvbellew/logs/" # include trailing "/"

    for option_tuple in optlist:
        if (option_tuple[0] == "--db"):       
            options["db"] = option_tuple[1]
        if (option_tuple[0] == "--csvDirectory"):       
            options["csvDirectory"] = option_tuple[1]

    current_user = getpass.getuser()
    db_info = configparser.RawConfigParser()
    db_info.read('/home/{user}/.my.cnf'.format(user=current_user))
    dbuser = db_info.get('client', 'user')
    dbpassword = db_info.get('client', 'password')

    qualtrics_info = configparser.RawConfigParser()
    qualtrics_info.read(options["myqualtricsconfig"] )
    qual_token = qualtrics_info.get("client", "token")
    qual_consentId = qualtrics_info.get("client", "survey_key_consent")
    qual_dataCenter = qualtrics_info.get("client", "dataCenter")   
    qual_format = qualtrics_info.get("client", "fileFormat")  
    qual_studyFile = qualtrics_info.get("client", "study_filename") 

    if options["testing"]:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_test_id")
    else:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_id")

    runtime = datetime.now(zoneinfo.ZoneInfo("UTC"))

    # # Get list of potential ids from consent file to start
    # # Get Qualtrics file:
    # qual_return = get_qualtrics_study_filehandle(qual_consentId, qual_token, qual_dataCenter, qual_format,qual_studyFile)

    # #Get Qualtrics file data:
    # if (qual_return["status"] == "success"):
    #     print("CONSENT:"+qual_consentId)
    #     user_df = pd.read_csv(qual_return["file_handle"], usecols=["ResponseId","time_zone","RecordedDate"], skiprows=[1,2])
    #     #user_df = pd.read_csv(qual_return["file_handle"], skiprows=[1,2])
    #     user_df["time_zone"] = user_df["time_zone"].fillna(0).astype(int)
    #     user_df["device_id"] = user_df["ResponseId"]
    #     user_df["signup_date"] = user_df["RecordedDate"].fillna(runtime)
    #     user_df["signup_date"] = pd.to_datetime(user_df["signup_date"], utc=True)
    #     user_df = user_df.drop("ResponseId", axis = 1)
    #     user_df = user_df.drop("RecordedDate", axis = 1)
    #     #print(user_df.sort_values("device_id").to_string())
    # else:
    #     print("Error loading Qualtrics data file.")
    #     print("   Message: "+qual_return["message"])
    #     print("Create Blank df version and continue.")
    #     user_df = pd.DataFrame(columns=["device_id","time_zone","signup_date"])

    # user_df = user_df.set_index("device_id")
    # #print(user_df)

    user_df = get_previous_baseline_users(options["csv_Baseline_Directory"] , runtime, options["days_to_get"], options["start_day_offset"])
    user_df = user_df.set_index("device_id")
    print(user_df)
    #exit()
    
   # chokes on "localhost" (Possible remote ssh issue?).  Force Connection type by using Loopback IP address.
    table_string = db.URL.create("mariadb+mariadbconnector",
                            host="127.0.0.1",
                            port=3306,
                            database = "information_schema",
                            username=dbuser,
                            password=dbpassword)

    table_engine = db.create_engine(url=table_string, pool_pre_ping=True)   

    report_df = pd.DataFrame(columns=["device_id", "country_code", "gps_type","entry_count","mean_latitude", "mean_longitude", "mean_latitude_difference", "mean_longitude_difference"])

    #print(user_df.index.values)
    #for device_id in [user_df.index.values[0]]:
    for device_id in user_df.index.values:
        with table_engine.connect() as table_connection: 
            # GET DISABLED STATISTICS
            sql_stmt = str("SELECT * from " +
                            options["locations_db"] + "." + options["locations_table"] +
                            " where device_id = '" +device_id + "'" +
                            " AND" +
                            " label = 'disabled'")
            table_df = pd.read_sql(sql_stmt,con=table_connection)
            if (len(table_df) > 0):
                group_by = table_df.groupby("provider")
                for indx, group_df in group_by:
                    report_entry_df = pd.DataFrame({"device_id": [group_df["device_id"].iloc[0]],
                                                    "country_code" : ["DISABLED"], 
                                                    "gps_type" : [indx],
                                                    "entry_count" :[len(group_df)],
                                                    "mean_latitude" : [np.NaN], 
                                                    "mean_longitude" : [np.NaN],
                                                    "mean_latitude_difference" : [np.NaN],
                                                    "mean_longitude_difference" :  [np.NaN]})
                    print(group_df["device_id"].iloc[0], "DISABLED", indx, len(group_df))

                    if (len(report_df) == 0):
                        report_df = report_entry_df
                    else:
                        report_df = pd.concat([report_df, report_entry_df], ignore_index = True)

            #GET ENABLED STATISTICS
            sql_stmt = str("SELECT * from " +
                            options["locations_db"] + "." + options["locations_table"] +
                            " where device_id = '" +device_id + "'" +
                            " AND " +
                            "NOT label = 'disabled'")
                            #" where device_id = 'R_6EH42QJmCiyDj8C'")
            table_df = pd.read_sql(sql_stmt,con=table_connection)
            if (len(table_df) > 0):
                table_df["reverse_geocode"] = table_df.apply(lambda x: reverse_geocode.get((x["double_latitude"], x["double_longitude"]))["country_code"], axis=1 )
                group_by = table_df.groupby(["reverse_geocode", "provider" ])
                for indx, group_df in group_by:
                    group_df["mean_lat"] = group_df["double_latitude"].mean()
                    group_df["mean_long"] = group_df["double_longitude"].mean()
                    group_df["diff_lat"] = group_df["double_latitude"] - group_df["mean_lat"] 
                    group_df["diff_long"] = group_df["double_longitude"] - group_df["mean_long"] 
                    group_df["diff_lat"] = group_df["diff_lat"].abs()
                    group_df["diff_long"] = group_df["diff_long"].abs()
                    mean_diff_lat = group_df["diff_lat"].mean()
                    mean_diff_long = group_df["diff_long"].mean()
                    report_entry_df = pd.DataFrame({"device_id": [group_df["device_id"].iloc[0]],
                                                    "country_code" : [indx[0]], 
                                                    "gps_type" : [indx[1]],
                                                    "entry_count" :[len(group_df)],
                                                    "mean_latitude" : [group_df["mean_lat"].iloc[0]], 
                                                    "mean_longitude" : [group_df["mean_long"].iloc[0]],
                                                    "mean_latitude_difference" : [mean_diff_lat],
                                                    "mean_longitude_difference" :  [mean_diff_long]})
                    print(group_df["device_id"].iloc[0], indx[0], indx[1], len(group_df), group_df["mean_lat"].iloc[0], group_df["mean_long"].iloc[0],  mean_diff_lat, mean_diff_long)

                    if (len(report_df) == 0):
                        report_df = report_entry_df
                    else:
                        report_df = pd.concat([report_df, report_entry_df], ignore_index = True)
    print(report_df)

    start_day_offset_delta = -timedelta(days=options["start_day_offset"])
    date_string_to_log = (runtime + start_day_offset_delta).strftime("%Y-%m-%d")    

    csv_base_name = str("GPS_LOCATION_DATA_FOR_BASELINES_CREATED_ON_"+
                        str(date_string_to_log) + "_" +
                        "AND_"+
                        str(options["days_to_get"] - 1) + "_" +
                        "DAYS_PRIOR_"
                        "runtime_" +
                        runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                        ".csv" )
    csv_file_name = str( options["csvDirectory"] + csv_base_name)

    write_columns = report_df.columns.values.tolist()
    #write_columns.remove("time_zone")
    report_df.to_csv(csv_file_name, sep = ",", columns = write_columns, header=True, index = False)  
    send_file_to_google_drive(options["csvDirectory"] , csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )  

def usage():
    print("python " + str(sys.argv[0]) + "--db <Database Name> --filter <All, DAY, 24HR, FULL>  \
                                          --startDay <Number of days ago to start pull (Default 1)> \
                                          --endDay <Number of days ago to end pull> (Default 0)\
                                          --csvDirectory <location to put completed data file> "
          ) 

def get_qualtrics_study_filehandle(survey_key, api_token, datacenter = "co1", format = "csv", default_filename = ""): 

    # Tell Qualtrics that we want a csv file
    qual_url = "https://"+datacenter+".qualtrics.com/API/v3/surveys/"+survey_key+"/export-responses/"
    qual_data = {"format" : format}
    qual_headers = {"X-API-TOKEN" : api_token,
                    "Content-Type": "application/json"}
    
    #print(qual_url)
    response = requests.post(url = qual_url, data = json.dumps(qual_data), headers = qual_headers)
    #print(response.text)


    progressId = response.json()["result"]["progressId"]

    # Check with Qualtrics to wait for data collection to be complete
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    while progressStatus != "complete" and progressStatus != "failed":
    #    print ("progressStatus=", progressStatus)
        requestCheckUrl = qual_url + progressId
        requestCheckResponse = requests.request("GET", requestCheckUrl, headers=qual_headers)
        requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete", flush=True)
        progressStatus = requestCheckResponse.json()["result"]["status"]

    # check for successful data collection
    if progressStatus == "failed":
        return {"file_handle" : None, "status":"failed", "message":"Failed to retrieve file."}

    fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file
    requestDownloadUrl = qual_url + fileId + '/file'
    requestDownload = requests.request("GET", requestDownloadUrl, headers=qual_headers, stream=True)

 
    # Step 4: Unzipping the file
    zf = zipfile.ZipFile(io.BytesIO(requestDownload.content))
    if (len(zf.namelist()) == 1):
        study_file_name = zf.namelist()[0]
    else:
        if (len(zf.namelist()) == 0):
            return {"file_handle" : None, "status":"failed", "message":"Qualtrics returned 0 files."}
        else:
            if (default_filename != ""):
                study_file_name = default_filename
            else:
                return {"file_handle" : None, "status":"failed", "message":"Qualtrics returned multiple files and no default set."}

    file_handle = zf.open(study_file_name)
    return {"file_handle" : file_handle, "status":"success", "message":"Qualtrics file: "+study_file_name+ "retrieved successfully"}

def send_file_to_google_drive(data_directory, filename, upload_directory_id, credentials_file, scope_directory):

    creds = Credentials.from_service_account_file(credentials_file, scopes=scope_directory)

    service = build("drive", "v3", credentials=creds)

    # Upload a file to the specified folder
    file_metadata = {"name": filename, "parents":[upload_directory_id]}
    media = MediaFileUpload(str(data_directory + filename), resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(F'File ID: "{file.get("id")}".')

def get_previous_baseline_users(csv_baseline_directory, runtime, days_to_get, start_day_offset):

    start_day_offset_delta = -timedelta(days=start_day_offset)

    return_df = pd.DataFrame(columns=["device_id"])
    for i in range(days_to_get):
        file_df = pd.DataFrame(columns=["device_id"])
        day_offset_delta = -timedelta(days=i)
        date_string_to_get = (runtime + start_day_offset_delta + day_offset_delta).strftime("%Y%m%d")

        #check old style Baseline file name
        files = glob.glob(csv_baseline_directory+"*_BASELINE_"+date_string_to_get+"_*.csv")
        if len(files) == 0:  
            #check new style Baseline file name
            date_string_to_get = (runtime + start_day_offset_delta + day_offset_delta).strftime("%Y-%m-%d")
            files = glob.glob(csv_baseline_directory+"BASELINE_COMPLETION_FOR_DAY_"+date_string_to_get+"_*.csv")

        if len(files) > 0:
            print(files)
            file_to_load = files[len(files) - 1]
            file_df = pd.read_csv(file_to_load, usecols=["study_id"])
            file_df.rename(columns={"study_id":"device_id"}, inplace=True)
            #print(date_string_to_get)
            #print(file_df)
            if (len(file_df) > 0):
                if (len(return_df) == 0):
                    return_df = file_df
                else:
                    return_df = pd.concat([return_df, file_df], ignore_index=True, axis=0)

    return return_df



if __name__ == "__main__":

    main()

