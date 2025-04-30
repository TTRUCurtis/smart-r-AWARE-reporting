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
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError


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

    options["debug_level"] = 0
    options["db"] = "OAUTH_7" # AWARE Database to analyze
    options["study_name"] = "Smart_R"
    options["filter_list"] = ["DAY", "24HR", "FULL"] # (Calendary Day, Previous 24 hrs, Total all time)
    options["filter"] = "DAY" # "ALL" will do all 3 collations
    options["startDay"] = 1   # How many days previously you want to start ( Generally 1 - for either the previous 24 hrs or previous calendar day)
    options["endDay"] = 0      # How many days previously you want to end (Generally 0 - for either the previous 24 hrs or previous calendar day)
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["myqualtricsconfig"] = str(str(Path.home()) + '/.my.qualtrics.cnf')
    options["filter_days_before_start"] = 2
    options["filter_days_after_end"] = 2
    options["create_daily_reminder_data"] = True
    options["send_daily_reminder_data"] = True
    options["daily_reminder_endpoint"] = "https://ttru-smg.wwbp.org/qualtrics/reminder/"
    options["create_daily_reporting_data"] = True
    options["send_daily_reporting_data"] = True
    options["create_daily_baseline_data"] = True
    options["send_daily_baseline_data"] = True
    options["create_weekly_reporting_data"] = True
    options["send_weekly_reporting_data"] = True
    options["weekly_reporting_date"] = 0  #0 is Monday, 1 Tuesday...
    options["daily_report_db"] = "REPORTING"
    options["db_reporting_table"] = "daily_response"
    options["db_baseline_table"] = "baseline_completed"
    options["timezone_info_table"] = "consent_timezone"
    options["payment_source_table"] = "daily_response"
    options["google_oauth_credentials_file"] = "/home/douglasvbellew/.smart-r-config.json"
    options["google_oauth_scope_directory"] = ["https://www.googleapis.com/auth/drive"]

    for option_tuple in optlist:
        if (option_tuple[0] == "--db"):       
            options["db"] = option_tuple[1]
        if (option_tuple[0] == "--filter"):       
            options["filter"] = option_tuple[1] 
        if (option_tuple[0] == "--startDay"):       
            options["startDay"] = option_tuple[1] 
        if (option_tuple[0] == "--endDay"):       
            options["endDay"] = option_tuple[1]
        if (option_tuple[0] == "--csvDirectory"):       
            options["csvDirectory"] = option_tuple[1]

    if options["filter"] != "ALL" :
        if options["filter"] in options["filter_list"]:
            options["filter_list"] = [ options["filter"] ]
        else:
            print("FATAL ERROR: UNKNOW FILTER TYPE: "+ str(options["filter"]))
            sys.exit(2)

    if options["testing"]:
        options["csvDirectory"] = "/home/douglasvbellew/test/" # include trailing "/"
        options["db_reporting_table"] = "daily_response_test"
        options["db_baseline_table"] = "baseline_completed_test"
        options["send_daily_reminder_data"] = False

    else:
        options["csvDirectory"] = "/home/douglasvbellew/logs/" # include trailing "/"

    print("Using Filter List:"+  str(options["filter_list"]))
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
    qual_baselineId = qualtrics_info.get("client", "survey_key_baseline") 
    if options["testing"]:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_test_id")
    else:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_id")

    runtime = datetime.now(zoneinfo.ZoneInfo("UTC"))

    # Get list of potential ids from consent file to start
    # Get Qualtrics file:
    qual_return = get_qualtrics_study_filehandle(qual_consentId, qual_token, qual_dataCenter, qual_format,qual_studyFile)

    #Get Qualtrics file data:
    if (qual_return["status"] == "success"):
        print("CONSENT:"+qual_consentId)
        user_df = pd.read_csv(qual_return["file_handle"], usecols=["ResponseId","time_zone","RecordedDate"], skiprows=[1,2])
        #user_df = pd.read_csv(qual_return["file_handle"], skiprows=[1,2])
        user_df["time_zone"] = user_df["time_zone"].fillna(0).astype(int)
        user_df["device_id"] = user_df["ResponseId"]
        user_df["signup_date"] = user_df["RecordedDate"].fillna(runtime)
        user_df["signup_date"] = pd.to_datetime(user_df["signup_date"], utc=True)
        user_df = user_df.drop("ResponseId", axis = 1)
        user_df = user_df.drop("RecordedDate", axis = 1)
        #print(user_df.sort_values("device_id").to_string())
    else:
        print("Error loading Qualtrics data file.")
        print("   Message: "+qual_return["message"])
        print("Create Blank df version and continue.")
        user_df = pd.DataFrame(columns=["device_id","time_zone","signup_date"])

    user_df = user_df.set_index("device_id")
    #print(user_df)


   # chokes on "localhost" (Possible remote ssh issue?).  Force Connection type by using Loopback IP address.
    table_string = db.URL.create("mariadb+mariadbconnector",
                            host="127.0.0.1",
                            port=3306,
                            database = "information_schema",
                            username=dbuser,
                            password=dbpassword)

    table_engine = db.create_engine(url=table_string, pool_pre_ping=True)   

    with table_engine.connect() as table_connection: 
        exclude_table_list = str ( 
                            "(" +
                             "'mqtt_history'" + "," +
                             "'aware_log'" + "," +
                             "'aware_device'" + "," +
                             "'aware_studies'" + "," +
                             "'fused_geofences'" + "," +
                             "'fused_geofences_data'" + "," +
                             "'accelerometer'" + "," +
                             "'" + options["db_reporting_table"] + "'" + ","
                             "'" + options["timezone_info_table"] + "'" +
                            ")"
        )
        #print(exclude_table_list)
        table_df = pd.read_sql(
                            "SELECT table_name from information_schema.tables " +
                            "where table_schema = '" + options["db"] + "'" +
                            #"AND table_name NOT IN ('mqtt_history', '" + options["db_reporting_table"] + "')",
                            "AND table_name NOT IN "+exclude_table_list,
                            con=table_connection
        )
    
    #print(table_df)

    if (len(table_df) > 0):
    # Put Result into PANDAS dataframe
        pd.set_option("max_colwidth",30)
        pd.set_option("large_repr", "truncate")
        pd.set_option("display.width", None)
        if (options["debug_level"] != 0):
            print("Retrieved "+ str(len(table_df))+ " rows.")

        #print(table_df)

        # Get AWARE Table names from information_schema database
        aware_string = db.URL.create("mariadb+mariadbconnector",
                                host="127.0.0.1",
                                port=3306,
                                database = options["db"],
                                username=dbuser,
                                password=dbpassword)

        aware_engine = db.create_engine(url=aware_string, pool_pre_ping=True)   

        # with aware_engine.connect() as aware_connection: 
        #     user_df = pd.read_sql(
        #                         "SELECT unique(device_id) from " +
        #                         options["db"] +
        #                         ".aware_device" ,
        #                         con=aware_connection
        #     )
            


        # #print(user_df)

        csv_file_name = ""
        if (len(user_df) > 0) :

            # Get Baseline file:
            qual_return = get_qualtrics_study_filehandle(qual_baselineId, qual_token, qual_dataCenter, qual_format,qual_studyFile)

            #Get Baseline file data:

            if (qual_return["status"] == "success"):
                qual_df = pd.read_csv(qual_return["file_handle"], usecols=["studyId","RecordedDate","good"], skiprows=[1,2])
                #qual_df = pd.read_csv(qual_return["file_handle"], skiprows=[1,2])
                print("BASELINE:"+qual_baselineId)
                #print(qual_df.sort_values("good").to_string())
                qual_df["studyId"] = qual_df["studyId"].fillna(" ")
                qual_df["baseline_complete"] = qual_df["good"].fillna(0).astype(int)
                qual_df["baseline_complete_date"] = qual_df["RecordedDate"].fillna("1970-01-01 00:00:00")
                qual_df["baseline_complete_date"] = pd.to_datetime(qual_df["baseline_complete_date"], utc=True)
                qual_df = qual_df.drop("RecordedDate", axis = 1)
                qual_df = qual_df.drop("good", axis = 1)

                # Reduce rows to the earliest "baseline_complete_date" for a completed "baseline_complete" (=1) for a specific "studyId"
                qual_df = qual_df.sort_values(by=["baseline_complete", "baseline_complete_date"], ascending=[False, True]).drop_duplicates("studyId").reset_index(drop=True)

            else:
                print("Error loading Qualtrics data file.")
                print("   Message: "+qual_return["message"])
                print("Create Blank df version and continue.")
                qual_df = pd.DataFrame(columns=["studyId","baseline_complete","baseline_complete_date"])

            qual_df = qual_df.set_index("studyId")

            #print(user_df.columns)
            #print(qual_df.columns)
            user_df = user_df.join(qual_df, on="device_id", how="left")
            user_df["baseline_complete"] = user_df["baseline_complete"].fillna(0).astype(int)
            user_df["baseline_complete_date"] = user_df["baseline_complete_date"].fillna("1970-01-01 00:00:00")
            user_df["baseline_complete_date"] = pd.to_datetime(user_df["baseline_complete_date"], utc=True)


            # Note these values will be negative, so adding days to them goes further into the past
            # Add 1 ms to the endday so only midnight of the wanted day is included. (BETWEEN is inclusive on both sides)
            start_day_offset = -timedelta(days=options["startDay"]) 
            end_day_offset   = -(timedelta(days=options["endDay"]) + timedelta(milliseconds=1))
            filter_day_start_offset = -timedelta(days=options["filter_days_before_start"]) 
            filter_day_end_offset = timedelta(days=options["filter_days_after_end"]) 

            #print(user_df.sort_index().to_string())
            user_df_base = pd.DataFrame(columns = user_df.columns.copy(), data = copy.deepcopy(user_df.values), index=user_df.index.copy())

            for filter in options["filter_list"]:
                print("Filter is: "+filter)
                user_df = pd.DataFrame(columns = user_df_base.columns.copy(), data = copy.deepcopy(user_df_base.values), index=user_df_base.index.copy())  # reinitialize for subsequent runs
                user_df = user_df.reset_index()
                #print(user_df)
                user_tz_df = user_df[["device_id", "time_zone"]]
                user_tz_df = user_tz_df.set_index("device_id")
                for table_row in range(len(table_df)):
                #for table_row in [6]: # TESTING
                    print("Gathering AWARE data for table:"+str(table_df.loc[table_row,"table_name"]))
                    #user_df[table_df.loc[table_row,"table_name"]] = np.zeros(len(user_df), dtype=int)
                    sensor_stack_df = pd.DataFrame(columns=["device_id", table_df.loc[table_row,"table_name"]])
                    sensor_stack_df = sensor_stack_df.set_index("device_id")
                    for tz_num in [1,2,3,4,5,6]:
                        if (filter == "DAY"):
                            # Get midnight in participant local time 
                            local_date = datetime.combine(runtime.date(), time.min, tzinfo= zoneinfo.ZoneInfo(get_local_tz_name(tz_num)))
                            #print(local_date)
                            sql_stmt = str("SELECT device_id, COUNT(*) FROM " + 
                                        options["db"] +
                                        "." +
                                        table_df.loc[table_row,"table_name"] +
                                        " " +
                                        "WHERE timestamp BETWEEN "+
                                        str(int((local_date + start_day_offset).timestamp() * 1000))  + 
                                        #"UNIX_TIMESTAMP(subdate(current_date, "+
                                        #str(options["startDay"]) +
                                        #"))*1000 " +
                                        " AND " +
                                        str(int((local_date + end_day_offset).timestamp() * 1000)) +
                                        #"UNIX_TIMESTAMP(subdate(current_date," +
                                        #str(options["endDay"]) +
                                        #"))*1000"
                                        " GROUP BY device_id"
                            )

                        elif (filter == "24HR"):
                            sql_stmt = str("SELECT device_id, COUNT(*) FROM " + 
                                        options["db"] +
                                        "." +
                                        table_df.loc[table_row,"table_name"] +
                                        " " +
                                        "WHERE timestamp BETWEEN " +
                                        str(int((runtime+start_day_offset).timestamp() * 1000)) +   
                                            # UNIX_TIMESTAMP(subdate(NOW(), "+
                                        #str(options["startDay"]) +
                                        #"))*1000 " +
                                        " AND " +
                                        str(int((runtime+end_day_offset).timestamp() * 1000)) +
                                        #" UNIX_TIMESTAMP(subdate(NOW()," +
                                        #str(options["endDay"]) +
                                        #"))*1000"
                                        " GROUP BY device_id"
                            )
                        else:
                            sql_stmt = str("SELECT device_id, COUNT(*) FROM " + 
                                        options["db"] +
                                        "." +
                                        table_df.loc[table_row,"table_name"] +
                                        " " +
                                        "GROUP BY device_id "
                            )


                        #insert into user_df[user, table_name]
                        with aware_engine.connect() as aware_connection: 
                            print(sql_stmt, flush = True)
                            sensor_df = pd.read_sql(
                                        sql_stmt,
                                        con=aware_connection
                                            )
                            sensor_df[table_df.loc[table_row,"table_name"]] = sensor_df["COUNT(*)"]
                            sensor_df = sensor_df.drop("COUNT(*)", axis = 1)
                            sensor_df = sensor_df.set_index("device_id")
                            sensor_df = sensor_df.join(user_tz_df, on="device_id", how="left")
                            sensor_df = sensor_df[sensor_df["time_zone"] == tz_num]
                            sensor_df = sensor_df.drop("time_zone", axis = 1)
                            if (tz_num == 1):
                                sensor_stack_df = sensor_df
                            else:
                                sensor_stack_df = pd.concat([sensor_stack_df, sensor_df], axis=0)
                            #print(sensor_stack_df)

                    #print(sensor_stack_df)
                    user_df = user_df.join(sensor_stack_df, on="device_id", how="left")
                    user_df.fillna({table_df.loc[table_row,"table_name"] : 0}, inplace=True)
                    user_df[table_df.loc[table_row,"table_name"]] = user_df[table_df.loc[table_row,"table_name"]].astype(int)
                    #print(user_df)

                #user_df = user_df.drop("time_zone", axis = 1)
                #print(user_df.sort_values("device_id").to_string(), flush = True)
                # User_data_DAY_20241204_194504.csv

                ## GET DAILY USER SURVEY DATA
                if ((filter == "DAY") or (filter == "24HR")):
                    # Figure out day of the week survey that we want:
                    weekday = (runtime+start_day_offset).strftime("%A").lower()
                    daily_survey_key = qualtrics_info.get("client", "survey_key_"+weekday)
                    data_date = (runtime+start_day_offset).date()
                    print(daily_survey_key)
                    print(data_date)

                    # Get Qualtrics file:
                    qual_return = get_qualtrics_study_filehandle(daily_survey_key, qual_token, qual_dataCenter, qual_format,"smart-r "+weekday+".csv")

                    #Get Qualtrics file data:
                    if (qual_return["status"] == "success"):
                        print("DAILY:"+daily_survey_key)
                        survey_df = pd.read_csv(qual_return["file_handle"], usecols=["studyId",'good','RecordedDate'], skiprows=[1,2])
                        survey_df["RecordedDate"] = pd.to_datetime(survey_df["RecordedDate"], format="%Y-%m-%d %H:%M:%S", utc=True)
                        survey_df["daily_survey_complete"] = survey_df["good"]

                    else:
                        print("Error loading Qualtrics data file.")
                        print("   Message: "+qual_return["message"])
                        print("Create Blank df version and continue.")
                        survey_df = pd.DataFrame(columns=["studyId",'daily_survey_complete','RecordedDate'])
                        survey_df["RecordedDate"] = pd.to_datetime(survey_df["RecordedDate"], format="%Y-%m-%d %H:%M:%S", utc=True)

                    #print(survey_df)

                    survey_df = survey_df[ (survey_df["RecordedDate"] >= (runtime + start_day_offset + filter_day_start_offset)) &
                                           (survey_df["RecordedDate"] <= (runtime + end_day_offset + filter_day_end_offset)) ]

                    #print(survey_df.sort_values("studyId"), flush = True)
                    survey_df = survey_df.sort_values(["good", "daily_survey_complete", "RecordedDate"], ascending=[False,False,True]).drop_duplicates(["studyId"])
                    #print(survey_df.sort_values("studyId"), flush = True)
                    survey_df = survey_df.set_index("studyId")
                    #print(survey_df.sort_index(), flush = True)

                    user_df = user_df.join(survey_df["daily_survey_complete"], on="device_id", how="left")
                    #print(user_df.sort_values("device_id").to_string(), flush = True)
                    user_df["daily_survey_complete"] = user_df["daily_survey_complete"].fillna(0).astype(int)

                    #Gather AWARE data from tables
                    user_df["Aware_Collected"] = user_df.apply(lambda x: check_aware_tables_for_completion(table_df, x), axis=1)
                    filtered_df = user_df[(user_df["Aware_Collected"] == 0) | (user_df["daily_survey_complete"] == 0)]
                    #print(user_df)
                    #print(filtered_df)

                    #print(user_df)
                    if (filter == "DAY"):
                        if options["create_daily_reminder_data"]:
                            #Send Aware and Survey Collection (failure) data to reminder endpoint

                            failure_data = {"data": [create_post_item(x,y,a,b) for x,y,a,b in zip(filtered_df.index, filtered_df["device_id"],filtered_df["Aware_Collected"],filtered_df["daily_survey_complete"])]}
                            
                            #print(json.dumps(failure_data))

                            if options["send_daily_reminder_data"]:
                                post_header = {"Auth_Token":"AUTHORIZATION_TOKEN_HERE"}
                                response = requests.post(options["daily_reminder_endpoint"], json=failure_data, headers=post_header)

                                #print(response.status_code)
                                #print(response.json())

                            json_base_name = str("PARTICIPANT_REMINDER_DATA_FOR_" +
                                                filter + 
                                                "_" +
                                                 (runtime + start_day_offset).strftime("%Y-%m-%d") + "_" + 
                                                "runtime_" +
                                                runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                                                ".json" )
                            json_file_name = str( options["csvDirectory"] + json_base_name)

                            # Write JSON to file
                            with open(json_file_name, 'w') as json_file:
                                json.dump(failure_data, json_file, indent=4)  
                            send_file_to_google_drive( options["csvDirectory"], json_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] ) 

                        if options["create_daily_reporting_data"]:

                            # Send Aware and Survey Collection data to database
                            report_String = db.URL.create("mariadb+mariadbconnector",
                                                    host="127.0.0.1",
                                                    port=3306,
                                                    database = options["daily_report_db"],
                                                    username=dbuser,
                                                    password=dbpassword)

                            report_engine = db.create_engine(url=report_String, pool_pre_ping=True)   
        
                            #with report_engine.connect() as report_connection: 
                            reporting_df = pd.DataFrame(columns =["study_id", "aware_complete", "survey_complete"], data = copy.deepcopy(user_df[["device_id","Aware_Collected", "daily_survey_complete"]].values))
                            #print(reporting_df)
                            reporting_df["study_name"] = options["study_name"]
                            reporting_df["data_date"] = data_date
                            reporting_df["timestamp"] = int(runtime.timestamp() * 1000000)
                            #print(reporting_df)
                            if options["send_daily_reporting_data"]:
                                reporting_df.to_sql(name=options["db_reporting_table"], con=report_engine, if_exists="append", index=False)


                            csv_base_name = str( "PARTICIPANT_COMPLETION_DATA_FOR_" +
                                                filter + 
                                                "_" +
                                                 (runtime + start_day_offset).strftime("%Y-%m-%d") + "_" + 
                                                "runtime_" +
                                                runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                                                ".csv" )
                            csv_file_name = str( options["csvDirectory"] + csv_base_name)

                            write_columns = reporting_df.columns.values.tolist()
                            #write_columns.remove("time_zone")
                            reporting_df.to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = False)
                            send_file_to_google_drive(options["csvDirectory"], csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )    

                        if options["create_daily_baseline_data"]: 
                            # Send Aware and Survey Collection data to database
                            report_String = db.URL.create("mariadb+mariadbconnector",
                                                    host="127.0.0.1",
                                                    port=3306,
                                                    database = options["daily_report_db"],
                                                    username=dbuser,
                                                    password=dbpassword)

                            report_engine = db.create_engine(url=report_String, pool_pre_ping=True)   
        
                            #with report_engine.connect() as report_connection: 
                            baseline_df = pd.DataFrame(columns =["study_id", "baseline_complete", "baseline_complete_date"], data = copy.deepcopy(user_df[["device_id","baseline_complete", "baseline_complete_date"]].values))
                            #print(baseline_df)
                            baseline_df = baseline_df[ (baseline_df["baseline_complete_date"] >= (runtime + start_day_offset)) &
                                           (baseline_df["baseline_complete_date"] <= (runtime + end_day_offset)) ]
                            #print(baseline_df)
                            baseline_df["study_name"] = options["study_name"]
                            baseline_df["data_date"] = data_date
                            baseline_df["timestamp"] = int(runtime.timestamp() * 1000000)
                            #print(baseline_df)
                            if options["send_daily_baseline_data"]:
                                baseline_df.to_sql(name=options["db_baseline_table"], con=report_engine, if_exists="append", index=False)



                            csv_base_name = str("BASELINE_COMPLETION_FOR_" +
                                                filter + 
                                                "_" +
                                                 (runtime + start_day_offset).strftime("%Y-%m-%d") + "_" + 
                                                "runtime_" +
                                                runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                                                ".csv" )
                            csv_file_name = str( options["csvDirectory"] + csv_base_name)

                            write_columns = baseline_df.columns.values.tolist()
                            #write_columns.remove("time_zone")
                            baseline_df.to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = False)     
                            send_file_to_google_drive(options["csvDirectory"], csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] ) 

                        if options["weekly_reporting_date"] == runtime.weekday():
                            prev_week = runtime - dateutil.relativedelta.relativedelta(weeks=1, days=(options["weekly_reporting_date"] + 1) ) #Monday is day 0, so need to go back 1 more day
                            
                            # Send Aware and Survey Collection data to database
                            report_String = db.URL.create("mariadb+mariadbconnector",
                                                    host="127.0.0.1",
                                                    port=3306,
                                                    database = options["daily_report_db"],
                                                    username=dbuser,
                                                    password=dbpassword)

                            report_engine = db.create_engine(url=report_String, pool_pre_ping=True) 
                            date_set = "("
                            for i in range(7):
                                if i > 0:
                                    date_set = date_set + ","
                                date_set = date_set + str(
                                    "'" +
                                    (prev_week + dateutil.relativedelta.relativedelta(days=i)).strftime("%Y-%m-%d") +
                                    "'"
                                )
                            date_set = date_set + ")"
                            print("Pull payment data for dates: " + date_set)
                            sql_stmt = str("SELECT * FROM " + 
                                        options["daily_report_db"] +
                                        "." +
                                        options["payment_source_table"] +
                                        #"study_reporting" + ## TEST -- REMOVE
                                        " WHERE data_date IN " +
                                        date_set + 
                                        " AND study_name = " +
                                        "'" +
                                        options["study_name"] +
                                        "'"
                                         )
                            #insert into user_df[user, table_name]
                            with aware_engine.connect() as aware_connection: 
                                print(sql_stmt, flush = True)
                                month_report_select_df = pd.read_sql(
                                                    sql_stmt,
                                                    con=aware_connection
                                                     )
                            
                            month_report_stacked_df = month_report_select_df.groupby(["data_date", "study_id"])[["aware_complete", "survey_complete"]].apply(np.maximum.reduce).reset_index()
                            month_report_df = pd.DataFrame()
                            month_report_df["study_id"] = month_report_stacked_df["study_id"].unique()
                            month_day_list = month_report_stacked_df["data_date"].unique()
                            month_report_df["days_to_pay"] = 0
                            month_report_df["aware_days_complete"] = 0
                            month_report_df["survey_days_complete"] = 0
                            for append_date in sorted(month_day_list):
                                    temp_df = month_report_stacked_df[month_report_stacked_df["data_date"] == append_date]
                                    temp_df = temp_df.set_index("study_id")
                                    month_report_df = month_report_df.join(temp_df, on="study_id")
                                    month_report_df = month_report_df.drop("data_date",axis=1)
                                    month_report_df["aware_complete"] = month_report_df["aware_complete"].fillna(0).astype(int)
                                    month_report_df["survey_complete"] = month_report_df["survey_complete"].fillna(0).astype(int)
                                    month_report_df["days_to_pay"] = month_report_df["days_to_pay"] + (month_report_df["aware_complete"] & month_report_df["survey_complete"])
                                    month_report_df["aware_days_complete"] = month_report_df["aware_days_complete"] + month_report_df["aware_complete"]
                                    month_report_df["survey_days_complete"] = month_report_df["survey_days_complete"] + month_report_df["survey_complete"]
                                    month_report_df.rename(columns={"aware_complete":str(append_date+"_aware"), "survey_complete":str(append_date+"_survey")}, inplace=True)

                            #print(month_report_df)

                            csv_base_name = str("PAYMENT_DATA_FOR_WEEK_STARTING" +
                                                "_"  + prev_week.strftime("%Y-%m-%d") + "_" +
                                                "runtime_" + runtime.strftime("%Y-%m-%d_%H:%M:%S") +
                                                ".csv" )
                            csv_file_name = str( options["csvDirectory"] + csv_base_name)

                            write_columns = month_report_df.columns.values.tolist()
                            #write_columns.remove("time_zone")
                            month_report_df.to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = False)    
                            send_file_to_google_drive(options["csvDirectory"], csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )

                            #if options["send_weekly_reporting_data"]:
                            #    continue


                    #print(user_df.sort_values(["daily_survey_complete", "Aware_Collected","signup_date"], ascending=[False, False, False]).to_string())  
                    ## WRITE DATA TO CSV FILE
                    #print("Create DAILY AWARE DATA:")
                    #print(user_df)
                    csv_base_name = str("AWARE_TABLE_AND_USER_COMPELTION_FOR_" +
                                        filter + 
                                        "_" +
                                        (runtime + start_day_offset).strftime("%Y-%m-%d") + "_" + 
                                        "runtime_" +
                                        runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                                        ".csv" )
                    csv_file_name = str( options["csvDirectory"] + csv_base_name)

                    write_columns = user_df.columns.values.tolist()
                    #write_columns.remove("time_zone")
                    user_df.sort_values(["daily_survey_complete", "Aware_Collected","signup_date"], ascending=[False, False, False]).to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = False)
                    send_file_to_google_drive( options["csvDirectory"], csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )
                else:
                    daylist = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]
                    #for a particular day, do 3 days before and 4 days after
                    start_day = {"sunday":"W-THU",
                                 "monday":"W-FRI",
                                 "tuesday":"W-SAT",
                                 "wednesday":"W-SUN",
                                 "thursday":"W-MON",
                                 "friday":"W-TUE",
                                 "saturday":"W-WED"}
                    survey_df = {}
                    for weekday in daylist:

                        daily_survey_key = qualtrics_info.get("client", "survey_key_"+weekday)
                        # Get Qualtrics file:
                        qual_return = get_qualtrics_study_filehandle(daily_survey_key, qual_token, qual_dataCenter, qual_format,"smart-r "+weekday+".csv")


                        #Get Qualtrics file data:
                        if (qual_return["status"] == "success"):
                            survey_df[weekday] = pd.read_csv(qual_return["file_handle"], usecols=["studyId",'good','RecordedDate'], skiprows=[1,2])
                            survey_df[weekday]["RecordedDate"] = pd.to_datetime(survey_df[weekday]["RecordedDate"], format="%Y-%m-%d %H:%M:%S", utc=True)
                            survey_df[weekday]["daily_survey_complete"] = survey_df[weekday]["good"]
                            survey_df[weekday]["daily_survey_complete"] = survey_df[weekday]["daily_survey_complete"].fillna(0).astype(int)
                            survey_df[weekday] = survey_df[weekday].drop("good", axis = 1)
                            survey_df[weekday] = survey_df[weekday].dropna(subset=["studyId"])
                            print(weekday+":", flush = True)
                            #print(survey_df[weekday], flush = True)
                        else:
                            print("Error loading Qualtrics data file.")
                            print("   Message: "+qual_return["message"])
                            print("Create Blank df version and continue.")
                            survey_df[weekday] = pd.DataFrame(columns=["studyId",'daily_survey_complete','RecordedDate'])
                            survey_df[weekday]["RecordedDate"] = pd.to_datetime(survey_df[weekday]["RecordedDate"], format="%Y-%m-%d %H:%M:%S", utc=True)

                        #Remove Weeky duplicates:
                        de_duped_df = pd.DataFrame(columns=["studyId",'daily_survey_complete','RecordedDate'])
                        test_groups = survey_df[weekday].groupby(pd.Grouper(key="RecordedDate", freq=start_day[weekday]))
                        for key, item in test_groups:
                            if len(item) > 0:
                                item = item.sort_values(["studyId", "daily_survey_complete", "RecordedDate"], ascending=[False,False,True]).drop_duplicates(["studyId"])
                                if len(de_duped_df) == 0:
                                    de_duped_df = item
                                else:
                                    de_duped_df = pd.concat([de_duped_df, item], axis=0)                            

                        #print(de_duped_df)
                        #print(survey_df[weekday], flush = True)
                        survey_df[weekday] = de_duped_df.groupby(["studyId"])["daily_survey_complete"].sum().reset_index().set_index("studyId")
                        #print(survey_df[weekday])
                        user_df = user_df.join(survey_df[weekday], on="device_id", how="left")
                        user_df[weekday] = user_df["daily_survey_complete"].fillna(0).astype(int)
                        user_df = user_df.drop("daily_survey_complete", axis = 1)
                        user_df[weekday+"_total"] =  user_df.apply(lambda x: 0 if x["baseline_complete"] == 0 else get_possible_daily_surveys( x["baseline_complete_date"], runtime, day_text_to_number(weekday)), axis=1)

                        #print(user_df)


#                        survey_df[weekday] = survey_df[weekday].set_index("studyId")


                    print(user_df.sort_values(["baseline_complete", "device_id"], ascending=[False, True]).to_string())  
                    ## WRITE DATA TO CSV FILE
                    csv_base_name = str("FULL_STUDY_COMBINED_DATA_THROUGH" +
                                        "_" +
                                        runtime.strftime("%Y-%m-%d_%H:%M:%S") +  
                                        ".csv" )
                    csv_file_name = str(options["csvDirectory"] +  csv_base_name)

                    write_columns = user_df.columns.values.tolist()
                    #write_columns.remove("time_zone")
                    user_df.sort_values(["baseline_complete", "device_id"], ascending=[False, True]).to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = False)
                    send_file_to_google_drive(options["csvDirectory"], csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )

        else:
            ("FATAL ERROR: No users found for database:" + options["db"])
            sys.exit(2)

    else:
        print("FATAL ERROR: No tables found for database:" + options["db"])  
        sys.exit(2)

    return
        
def usage():
    print("python " + str(sys.argv[0]) + "--db <Database Name> --filter <All, DAY, 24HR, FULL>  \
                                          --startDay <Number of days ago to start pull (Default 1)> \
                                          --endDay <Number of days ago to end pull> (Default 0)\
                                          --csvDirectory <location to put completed data file> "
          ) 

def check_aware_tables_for_completion(table_df,  user):
    return_val = 0

    for table_row in range(len(table_df)): 
    #for table_row in [6]: #TESTING
        if (user[table_df.loc[table_row,"table_name"]] > 0):
            return_val = 1
            break

    return return_val

def create_post_item(index,studyId,AWARE,survey):
    return {"index":index, "study_id":studyId, "AWARE":AWARE, "survey":survey}

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

def get_local_tz_name(tz_num):
    timezones = ['America/Puerto_Rico', 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Arizona' ,'US/Pacific','US/Alaska','US/Hawaii' ]
    match tz_num:
        case 1: 
            return 'US/Eastern'
        case 2: 
            return 'US/Central'           
        case 3: 
            return 'US/Mountain'          
        case 4: 
            return 'US/Pacific'           
        case 5: 
            return 'US/Alaska'           
        case 6: 
            return 'US/Hawaii'
        case 7: 
            return 'America/Puerto_Rico'
        case 8: 
            return 'US/Arizona'
        case _: 
            return 'US/Eastern'  # Default to system timezone         

def get_possible_daily_surveys( start, end, week_day):
    #print(">>>>>>>>>")
    start_day = datetime.strptime(str(start.astimezone(zoneinfo.ZoneInfo('US/Eastern')).strftime("%Y%m%d")+" 00:00:00+0000"), "%Y%m%d %H:%M:%S%z") + timedelta(days=1)  #No survey on Baseline day
    end_day = datetime.strptime(str(end.astimezone(zoneinfo.ZoneInfo('US/Eastern')).strftime("%Y%m%d")+" 00:00:00+0000"), "%Y%m%d %H:%M:%S%z") - timedelta(days=1)
    #print(start_day)
    #print(end_day)
    num_weeks, remainder = divmod( (end_day-start_day).days, 7)

    if ( end_day.weekday() - week_day ) % 7 <= remainder:
       return num_weeks + 1
    else:
       return num_weeks

def day_text_to_number(day_text):
    # Define a dictionary mapping day names to their corresponding numbers
    days = {
        'monday': 0,
        'tuesday': 1,
        'wednesday': 2,
        'thursday': 3,
        'friday': 4,
        'saturday': 5,
        'sunday': 6
    }
    # Convert the input text to title case to match the dictionary keys
    day_text = day_text.lower()
    return days.get(day_text, "Invalid day name")

# def send_file_to_google_drive(filename, upload_directory_id, credentials_file, scope_directory):

#     creds = Credentials.from_service_account_file(credentials_file, scopes=scope_directory)

#     service = build("drive", "v3", credentials=creds)

#     # Upload a file to the specified folder
#     file_metadata = {"name": filename, "parents":[upload_directory_id]}
#     media = MediaFileUpload(filename, resumable=True)
#     file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
#     print(F'File ID: "{file.get("id")}".')

def send_file_to_google_drive(data_directory, filename, upload_directory_id, credentials_file, scope_directory):

    creds = Credentials.from_service_account_file(credentials_file, scopes=scope_directory)

    service = build("drive", "v3", credentials=creds)

    # Upload a file to the specified folder
    file_metadata = {"name": filename, "parents":[upload_directory_id]}
    media = MediaFileUpload(str(data_directory + filename), resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(F'File ID: "{file.get("id")}".')

if __name__ == "__main__":

    main()

