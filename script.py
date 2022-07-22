from boto3.session import Session
import boto3
import os

ACCESS_KEY = 'AKIA3KWFUA6S7R3H2ZPQ'
SECRET_KEY = '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'


session = boto3.client('s3', aws_access_key_id=ACCESS_KEY , aws_secret_access_key=SECRET_KEY)


session.download_file('iiqtest','1636994821620_insightiq_export_1636727116.zip',os.path.join(os.getcwd(), "1636994821620_insightiq_export_1636727116.zip"))