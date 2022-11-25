import boto3
import os
from decouple import config

from google.cloud import storage

from os import pardir,path

import psycopg2 
from queries import query_create_table
import json
from os import path as os_path

ACCESS_KEY = config('ACCESS_KEY_ID')
SECRET_KEY = config('SECRET_ACCESS_KEY')

def download_from_s3():
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY , aws_secret_access_key=SECRET_KEY)
    s3.download_file('iiqtest','1636994821620_insightiq_export_1636727116.zip',os.path.join(os.getcwd(), "1636994821620_insightiq_export_1636727116.zip"))

def upload_to_s3():
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY , aws_secret_access_key=SECRET_KEY)
    s3.upload_file(
    Filename="insightiq_export_1636727116/DASHISILON_6805ca0e04067358a752ac03776323443e24_config.json",
    Bucket="iiqtest",
    Key="DASHISILON_6805ca0e04067358a752ac03776323443e24_config.json",
)

def upload_to_gcp(file): 
    blob_name= file
    path_file = "./"+file
    service_account_file_path  = "./gcp.json"
    storage_client = storage.Client.from_service_account_json(service_account_file_path)
    bucket = storage_client.bucket(config('GS_BUCKET_NAME'))
    blob = bucket.blob(blob_name)
    # define default size of the blob
    storage.blob._DEFAULT_CHUNKSIZE = 1024*1024*2 # 1024 * 1024 B * 2 = 2 MB
    storage.blob._MAX_MULTIPART_SIZE = 1024*1024*2 # 2 MB
    with open(path_file, 'rb') as f:
        blob.upload_from_file(f)
    # then we create BigQuery table

def download_file_from_gcp(source_blob_name,destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(config('GS_BUCKET_NAME'))
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

# at first we connect to the postgresql server
DB_NAME="rfa"
DB_USER="postgres"
DB_PASSWORD="abDX69x6YHrurkyio0aK"
DB_HOST="34.66.214.223"
DB_PORT=5432

json_file_name = "W_FileAnalysis_03-08-2022-13_51_22.json"
json_file_path = os_path.join(".", "data", json_file_name)
table_name = json_file_name.split(".")[0]

class UploadJsonToSql:

    def __init__(self):
        self.connection = self.connectToDatabase()
        if self.connection is not None:
            self.uploadJsonToSqlServer()

    def connectToDatabase(self):
        try:
            conn = psycopg2.connect(host=DB_HOST, port = DB_PORT, 
                            database=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, client_encoding ='auto')
            return conn
        except Exception as e:
            print("error when trying to connect to the database" + repr(e))
            return None

    def create_table(self,table_name):
        try:
            query = query_create_table(table_name)
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except Exception as e:
            cursor.close()
            print("error when creating the table"+ repr(e))
            return True
        else:
            return True

    def openJsonFile(self):
        try:
            #json_file_path = "G:\\free-work\\datavoss\\RFA_json_to_table\\data\F_FileAnalysis_11-10-2022-13_52_13.json"
            initialConfig = ("SET statement_timeout = 0;"\
                                "SET lock_timeout = 0;"\
                                "SET idle_in_transaction_session_timeout = 0;"\
                                "SET client_encoding = 'UTF8';"\
                                "SET standard_conforming_strings = on;"\
                                "SELECT pg_catalog.set_config('search_path', '', false);"\
                                "SET check_function_bodies = false;"\
                                "SET xmloption = content;"\
                                "SET client_min_messages = warning;"\
                                "SET row_security = off;")
            query_insert = ('insert into public."'+table_name+'"( '\
                    ' "Mod_Age", "Dormant_Age_Sort", "Dormant_Age_Group", "Dormant_Age", "File_Age_Sort", "File_Age_Group", "'\
                    'File_Age", "File_Size_Group", "File_Size", "Last_Write_Time", "Last_Access_Time", "Creation_Time", "'\
                    'Drive_Label", "Username", "Drive_Serial_Number", "File_Size_Sort", "System_File", "Extension", "'\
                    'Server_Name", "User_Group", "File_Depth", "Directory", "File_Name", "File_Category", "Compressed") '\
                    ' VALUES \n')
            with open(json_file_path, 'r') as f:
                sql_file = open(table_name+".sql", "w")
                sql_file.write(query_insert)
                i = 0
                for line in f:
                    row_data = json.loads(line)
                    if i > 50000:
                        #break
                        i = 0
                        query = query_insert
                        sql_file.write(";\n"+query)
                    if i > 0:
                        i += 1
                        query = (" ,('"+str(row_data['Mod_Age'])+"', '"+str(row_data['Dormant_Age_Sort'])+"', '"+str(row_data['Dormant_Age_Group'])+"', '"+str(row_data['Dormant_Age'])+"', "\
                        " '"+str(row_data['File_Age_Sort'])+"', '"+str(row_data['File_Age_Group'])+"', '"+str(row_data['File_Age'])+"', '"+str(row_data['File_Size_Group'])+"', "\
                        " '"+str(row_data['File_Size'])+"', '"+str(row_data['Last_Write_Time'])+"', '"+str(row_data['Last_Access_Time'])+"', "\
                        " '"+str(row_data['Creation_Time'])+"', '"+str(row_data['Drive_Label'])+"', '"+str(row_data['Username']).replace("'","''")+"', '"+str(row_data['Drive_Serial_Number']).replace("'","''")+"', "\
                        " '"+str(row_data['File_Size_Sort'])+"', '"+str(row_data['System_File'])+"', '"+str(row_data['Extension'])+"', '"+str(row_data['Server_Name']).replace("'","''")+"', "\
                        " '"+str(row_data['User_Group']).replace("'","''")+"', '"+str(row_data['File_Depth'])+"', '"+str(row_data['Directory']).replace("'","''")+"', '"+str(row_data['File_Name']).replace("'","''")+"', "\
                        " '"+str(row_data['File_Category'])+"', '"+str(row_data['Compressed'])+"')\n")
                    else:
                        i += 1
                        query = (" ('"+str(row_data['Mod_Age'])+"', '"+str(row_data['Dormant_Age_Sort'])+"', '"+str(row_data['Dormant_Age_Group'])+"', '"+str(row_data['Dormant_Age'])+"', "\
                        " '"+str(row_data['File_Age_Sort'])+"', '"+str(row_data['File_Age_Group'])+"', '"+str(row_data['File_Age'])+"', '"+str(row_data['File_Size_Group'])+"', "\
                        " '"+str(row_data['File_Size'])+"', '"+str(row_data['Last_Write_Time'])+"', '"+str(row_data['Last_Access_Time'])+"', "\
                        " '"+str(row_data['Creation_Time'])+"', '"+str(row_data['Drive_Label'])+"', '"+str(row_data['Username']).replace("'","''")+"', '"+str(row_data['Drive_Serial_Number']).replace("'","''")+"', "\
                        " '"+str(row_data['File_Size_Sort'])+"', '"+str(row_data['System_File'])+"', '"+str(row_data['Extension'])+"', '"+str(row_data['Server_Name']).replace("'","''")+"', "\
                        " '"+str(row_data['User_Group']).replace("'","''")+"', '"+str(row_data['File_Depth'])+"', '"+str(row_data['Directory']).replace("'","''")+"', '"+str(row_data['File_Name']).replace("'","''")+"', "\
                        " '"+str(row_data['File_Category'])+"', '"+str(row_data['Compressed'])+"')\n")
                    try:
                        sql_file.write(query)
                    except Exception as e:
                        print("error when writing query to sql"+repr(e))
                        pass
                sql_file.close()
                f.close()
        except Exception as e:
            print("error when trying to open the json file"+repr(e))
            return False
        else:
            return True

    def uploadJsonToSqlServer(self):
        report = self.create_table(table_name)
        if report:
            self.openJsonFile()
            print("ok")
        else:
            print("not created")

    def cleanString(self,list_strings):
        print(list_strings)
        for string in list_strings:
            string_value = list_strings[string]
            if isinstance(string, str):
                if "'" in string_value :
                    print(string_value)
                    string_value = string_value.replace("'","''")
                    list_strings[string] = string_value
        return list_strings

upload = UploadJsonToSql()

