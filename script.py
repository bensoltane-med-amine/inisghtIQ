import boto3
import os
from decouple import config

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

upload_to_s3()

