""" 
Name    : audio_transcribe
Author  : Nasreen Khan Pattan
Date    : 2nd January 2020
IAM Role: audio_transcribe_lambda_iam

Description:
------------
    This lambda will invoke automatically when s3 bucket "audioFile" uploads audio files. this will transcribe upload file (one or multiple) 
    it will create a job id individual uploaded file and start transcription according to it and stores the resultant json file will be 
    uploaded into "transcribeFile". 
    
    In this Code the modifidable valriables are 
        s3 Source location in Trigger
        s3 Destination location in code
        Language Code in Code

"""

import json
import time
import boto3
from urllib.request import urlopen

def lambda_handler(event, context):
    transcribe = boto3.client("transcribe")     # Creating client for Transcribe Job
    s3 = boto3.client("s3")                     # Creating client for s3 Client
    
    # event handler while s3 bucket is added with new audio file to the bucket
    
    if event:
        # file_obj = event["Records"][0]                              # creating a list from s3 file uploads
        # bucket_name = str(file_obj["s3"]["bucket"]["name"])         # fetching Bucket name of S3
        # file_name = str(file_obj["s3"]["object"]["key"])            # fetching audio file name from uploads
        # s3_uri = create_uri(bucket_name,file_name)                  # creating file uri from s3 to fetch
        # file_type = file_name.split(".")[1]                         # fetching extension from S3 uploads
        # job_name = context.aws_request_id                           # Creating Job id using AWS request 
        
        records = [x for x in event.get('Records', []) if x.get('eventName') == 'ObjectCreated:Put']
        sorted_events = sorted(records, key=lambda e: e.get('eventTime'))
        latest_event = sorted_events[-1] if sorted_events else {}
        info = latest_event.get('s3', {})
        file_key = info.get('object', {}).get('key')
        bucket_name = info.get('bucket', {}).get('name')
        file_name =file_key.replace("%3A",":")
        s3_uri = create_uri(bucket_name,file_name)                  # creating file uri from s3 to fetch
        file_type = file_key.split(".")[1]                         # fetching extension from S3 uploads
        job_name = context.aws_request_id                           # Creating Job id using AWS request 
        
        print("latest_event",latest_event)
        print("Info", info)
        print("File_Key",file_key)
        print("Bucket_name",bucket_name)    
        print("S3 URL: ", s3_uri)
        
        """ 
            starting transcription job using transcribe API 
            
                we required job id with Transcription job, as per the audio file we have to provide the language code.
        """
       
        """ Single Audio Channel identification """
        # transcribe.start_transcription_job(TranscriptionJobName=job_name,
        #                                     Media = {"MediaFileUri": s3_uri},
        #                                     MediaFormat = file_type,
        #                                     OutputBucketName = "transcribes-audio",
        #                                     LanguageCode = "en-US")
        """ """
        
        """ User and Agent Audio Channel identification """
        transcribe.start_transcription_job(TranscriptionJobName=job_name,
                                            Media = {"MediaFileUri": s3_uri},
                                            MediaFormat = file_type,
                                            OutputBucketName = "transcribes-audio",
                                            LanguageCode = "en-US",
                                            Settings = {"ShowSpeakerLabels": True,
                                                "MaxSpeakerLabels": 2,
                                            })
        """ """                   
                                            
        # while True:
        #     status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        #     if status["TranscriptionJob"]["TranscriptionJobStatus"] in ["COMPLETED", "FAILED"]:
        #         break
        #     print("It's in Progress")
        #     time.sleep(5)
            
        # load_url = urlopen(status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"])
        # load_json = json.dumps(json.load(load_url))
        
        
        # """
        #     Uploading the file in to destination s3 bucket with json format
        # """
        # s3.put_object(Bucket = bucket_name, Key = "transcribeFile/{}.json".format(job_name), Body= load_json)
        
    
    #TODO implement
    return {
        'statusCode' : 200,
        'body' : json.dumps('Hello from lambda!')
    }


""" 
    Funtion to concate the s3 bucket and file names to make accessable uri
"""
def create_uri(bucket_name, file_name):
    return "s3://"+bucket_name+"/"+file_name
