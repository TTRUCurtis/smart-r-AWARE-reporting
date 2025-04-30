import sys
import os
import datetime as dt
import getopt
from pathlib import Path
import getpass
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date, time
import zoneinfo
import tzdata
import configparser
import io
import csv
import sqlalchemy as db
import mariadb
import glob
import json
from collections import Counter
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import pickle
from workersV2 import TextWorker

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
    options["days_to_get"] = 50
    options["start_day_offset"] = 1
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["myqualtricsconfig"] = str(str(Path.home()) + '/.my.qualtrics.cnf')
    options["sms_db"] = "OAUTH_7"
    options["sms_table"] = "plugin_sms"
    options["google_oauth_credentials_file"] = "/home/douglasvbellew/.smart-r-config.json"
    options["google_oauth_scope_directory"] = ["https://www.googleapis.com/auth/drive"]
    options["vector_pickle_name"] = "User_SMS_Vectors.pkl"
    options["max_display_ngrams"] = 100
    options["max_vector_ngrams"] = 5000
    options["device_id_exclude_list"] = ['R_3BBWeuKvQaXfQ9r']

    if options["testing"]:
        options["csvDirectory"] = "/home/douglasvbellew/cos_test/" # include trailing "/"
        options["csv_Baseline_Directory"] = "/home/douglasvbellew/logs/" # include trailing "/"
        options["vector_pickle_directory"] = "/home/douglasvbellew/cos_test/"
    else:
        options["csvDirectory"] = "/home/douglasvbellew/cos_logs/" # include trailing "/"
        options["csv_Baseline_Directory"] = "/home/douglasvbellew/logs/" # include trailing "/"
        options["vector_pickle_directory"] = "/home/douglasvbellew/cos_logs/"

    qualtrics_info = configparser.RawConfigParser()
    qualtrics_info.read(options["myqualtricsconfig"] )

    if options["testing"]:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_test_id")
    else:
        google_upload_drive_id = qualtrics_info.get("client", "google_upload_drive_id")

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

    runtime = datetime.now(zoneinfo.ZoneInfo("UTC"))

    # if os.path.exists(options["vector_pickle_directory"] + options["vector_pickle_name"]):
    #     print("pickle file exists:")
    #     with (open(options["vector_pickle_directory"] + options["vector_pickle_name"], "rb")) as pkl_file:
    #         vector_df = pickle.load(pkl_file)
    # else:
    #     print("pickle file doesn't exist.")
    #     vector_df = pd.DataFrame(columns=["device_id", "sms_topic_vector"])

    # print("Read in:"+str(len(vector_df))+" records.")
    
    
    user_df = get_previous_baseline_users(options["csv_Baseline_Directory"] , runtime, options["days_to_get"], options["start_day_offset"])
    user_df = user_df[~user_df["device_id"].isin(options["device_id_exclude_list"]) ].reset_index(drop=True)
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

    tok = TextWorker()

    device_list = list()
    counter_list = list()
    top_n_ngrams_list = list()
    vector_list = list()

    for device_id in user_df.index.values:
        device_list.append(device_id)
        vector_list.append([])
        table_df = pd.DataFrame(columns=["body"])
        sms_text = ""
        print("Processing device_id: "+device_id)
        with table_engine.connect() as table_connection: 
            sql_stmt = str("SELECT body from " +
                            options["sms_db"] + "." + options["sms_table"] +
                            " where device_id = '" +device_id + "'"
            )
            table_df = pd.read_sql(sql_stmt,con=table_connection)
            #print(table_df.to_string())
            if (len(table_df) > 0):
                sms_text = table_df["body"].str.cat(sep=" ")

                ngrams_person = tok.extractNgramPerDoc(sms_text)
                counter_list.append(ngrams_person)


                #Find Top options["max_display_ngrams"] grams for user
                sorted_ngrams_person = sorted(ngrams_person.items(), key = lambda kv: (kv[1], kv[0]), reverse=True)
                top100_items = ""
                for i in range(len(sorted_ngrams_person)):
                    key, val = sorted_ngrams_person[i]
                    if i < options["max_display_ngrams"]:
                        if i == 0:
                            #top100_items = key + ":" + str(val)
                            top100_items = key
                        else:
                            #top100_items = top100_items + ";" + key + ":" + str(val)
                            top100_items = top100_items + " ; " + key
                    else:
                        break
                top_n_ngrams_list.append(top100_items)
                #print(top100_items)
            else:
                counter_list.append(Counter([]))
                top_n_ngrams_list.append("")


    vector_df = pd.DataFrame({"device_id": device_list, "counter": counter_list, "top_ngrams": top_n_ngrams_list, "vector" : vector_list} )

    #print(vector_df.to_string())

    #Get the top 5000 ngrams
    total_counter = Counter([])
    for i in range(len(vector_df)):
        total_counter.update(vector_df.loc[i, "counter"])

    sorted_ngrams_total = sorted(total_counter.items(), key = lambda kv: (kv[1], kv[0]), reverse=True)
    top_ngrams = ["" for x in range(options["max_vector_ngrams"])]
    top_ngrams_weight = [0 for x in range(options["max_vector_ngrams"])]

    for i in range(len(sorted_ngrams_total)):
        key, val = sorted_ngrams_total[i]
        if i < options["max_vector_ngrams"]:
            top_ngrams[i] = key
            top_ngrams_weight[i] = val
            i = i + 1
        else:
            break
    print(top_ngrams)
    print(top_ngrams_weight)

    #Create 5000 item vector for each user
    for i in range(len(vector_df)):
        vector = np.zeros(options["max_vector_ngrams"])

        for j in range(options["max_vector_ngrams"]):
                vector[j] = vector_df.loc[i,"counter"][top_ngrams[j]]

        vector = vector / sum(vector)

        vector_df.loc[i,"vector"] = vector

    print(vector_df.to_string())      

    reporting_df = pd.DataFrame(index = vector_df["device_id"].to_numpy(), columns=vector_df["device_id"].to_numpy())

    #print(reporting_df)

    for i in range(len(vector_df)):
        #reporting_df.loc[ vector_df.loc[i,"device_id"], vector_df.loc[i,"device_id"]] = 1.0

        for j in range(i , len(vector_df)):
            cos_similarity_ij = np.dot(vector_df.loc[i, "vector"], vector_df.loc[j, "vector"]) / (np.linalg.norm(vector_df.loc[i, "vector"]) * np.linalg.norm(vector_df.loc[j, "vector"]))
            cos_similarity_ji = np.dot(vector_df.loc[j, "vector"], vector_df.loc[i, "vector"]) / (np.linalg.norm(vector_df.loc[j, "vector"]) * np.linalg.norm(vector_df.loc[i, "vector"]))

            reporting_df.loc[ vector_df.loc[i,"device_id"], vector_df.loc[j,"device_id"]] = cos_similarity_ij
            reporting_df.loc[ vector_df.loc[j,"device_id"], vector_df.loc[i,"device_id"]] = cos_similarity_ji

    #print(reporting_df)
    reporting_df.insert(loc=0, column = "Max Value", value=np.zeros(len(reporting_df)))
    reporting_df.insert(loc=1, column = "Mean Value", value=np.zeros(len(reporting_df)))
    reporting_df.insert(loc=2, column = "Min Value", value=np.zeros(len(reporting_df)))
    reporting_df.insert(loc=3, column = "Top 100 Terms", value=[x for x in top_n_ngrams_list])

    for i in range(len(reporting_df)):
        stats_df = pd.DataFrame(reporting_df.iloc[i])  #Create a Series from Each Row because we need to remove a different column from each row (namely the column named the same as the index)

        stats_df.drop([ "Max Value", "Mean Value", "Min Value","Top 100 Terms", reporting_df.index.values[i] ], inplace=True) # Remove Columns we don't want to include in the calculations

        reporting_df.loc[reporting_df.index.values[i],"Max Value"] = stats_df.max(skipna=True).iloc[0]     #Because of named index, returning a non-scalar value
        reporting_df.loc[reporting_df.index.values[i],"Mean Value"] = stats_df.mean(skipna=True).iloc[0]
        reporting_df.loc[reporting_df.index.values[i],"Min Value"] = stats_df.min(skipna=True).iloc[0]

    print(reporting_df.to_string())

    start_day_offset_delta = -timedelta(days=options["start_day_offset"])
    date_string_to_log = (runtime + start_day_offset_delta).strftime("%Y-%m-%d")    

    csv_base_name = str("SMS_SIMILARITY_DATA_FOR_BASELINES_CREATED_ON_"+
                        str(date_string_to_log) + "_" +
                        "AND_"+
                        str(options["days_to_get"]-1) + "_" +
                        "DAYS_PRIOR_"
                        "runtime_" +
                        runtime.strftime("%Y-%m-%d_%H:%M:%S") + 
                        ".csv" )
    
    csv_file_name = str( options["csvDirectory"] + csv_base_name)

    write_columns = reporting_df.columns.values.tolist()
    #write_columns.remove("time_zone")
    #reporting_df.to_csv(csv_file_name, sep = ",", na_rep="0", columns = write_columns, header=True, index = True)  
    reporting_df.to_csv(csv_file_name, sep = ",", columns = write_columns, header=True, index = True)  
    send_file_to_google_drive(options["csvDirectory"] , csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )  

    #send_file_to_google_drive(options["csvDirectory"] , csv_base_name, google_upload_drive_id, options["google_oauth_credentials_file"], options["google_oauth_scope_directory"] )  

def usage():
    print("python " + str(sys.argv[0]) + "--db <Database Name> --filter <All, DAY, 24HR, FULL>  \
                                          --startDay <Number of days ago to start pull (Default 1)> \
                                          --endDay <Number of days ago to end pull> (Default 0)\
                                          --csvDirectory <location to put completed data file> "
          ) 

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
