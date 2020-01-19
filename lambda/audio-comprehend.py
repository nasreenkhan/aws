import json
import boto3
import time


s3 = boto3.client('s3')
comprehend = boto3.client("comprehend")


def lambda_handler(event, context):
    if event:
        s3_object = event["Records"][0]["s3"]
        bucket_name = s3_object["bucket"]["name"]
        file_name_full = s3_object["object"]["key"]
        file_name = file_name_full.split(".")[0]
        
        print(file_name_full)
        print(file_name)
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_name_full)
        transcript_result = json.loads(file_obj['Body'].read())
        
        try:
            """ Comprehend logic for agent and SPeaker """
            
            agent = transcript_result["spk_1"]
            customer = transcript_result["spk_0"]
            
            response_agent = sentiment_analysis(agent)
            response_customer = sentiment_analysis(customer)
            
            agent = [average_sentiment(response_agent, "agent",file_name)]
            customer = [average_sentiment(response_customer, "customer",file_name)]
            
            with open("/tmp/response.json","w") as f:
                json.dump(agent,f)
                f.write("\n")
                json.dump(customer,f)
            
            s3.put_object(Bucket="comprehand-audio", Key=file_name, Body=open("/tmp/response.json").read())
        except KeyError:
            """ Comprehend logic only for one speaker """
             #
            #paragraph = transcript_result["results"]["transcripts"][0]["transcript"]
            # response = comprehend.batch_detect_sentiment(
            #     TextList=data_chunk(paragraph),
            #     LanguageCode='en'
            # )
            
            # final_response = average_sentiment(response)
    
            # s3.put_object(Bucket="comprehand-audio", Key=file_name, Body=final_response)
            customer = transcript_result["spk_0"]
            
            response_customer = sentiment_analysis(customer)
            
            customer = [average_sentiment(response_customer, "customer",file_name)]
            
            with open("/tmp/response.json","w") as f:

                f.write("\n")
                json.dump(customer,f)
            
            s3.put_object(Bucket="comprehand-audio", Key=file_name, Body=open("/tmp/response.json").read())
            

       

def sentiment_analysis(paragraph):
    response = comprehend.batch_detect_sentiment(
            TextList=data_chunk(paragraph),
            LanguageCode='en'
        )
    return response    

def data_chunk(paragraph, chunk_size=5000):
    # chunk the data due to comprehend limitation
    text_list = []
    while paragraph:
        text_list.append(str(paragraph[:chunk_size]))
        paragraph = paragraph[chunk_size:]
    return text_list


def average_sentiment(response):
    # averaging sentiment score
    positive, negative, neutral, mixed = 0, 0, 0, 0

    for score in response["ResultList"]:
        positive += score["SentimentScore"]["Positive"]
        negative += score["SentimentScore"]["Negative"]
        neutral += score["SentimentScore"]["Neutral"]
        mixed += score["SentimentScore"]["Mixed"]

    total_record = len(response["ResultList"])

    mapping = {
        "POSITIVE": positive / total_record,
        "NEGATIVE": negative / total_record,
        "NEUTRAL": neutral / total_record,
        "MIXED": mixed / total_record
    }

    response = json.dumps([{'Sentiment': max(mapping, key=mapping.get),
                            'SentimentScore': {
                                'Positive': mapping["POSITIVE"],
                                'Negative': mapping["NEGATIVE"],
                                'Neutral': mapping["NEUTRAL"],
                                'Mixed': mapping["MIXED"]
    }}])
    return response

def average_sentiment(response, response_type,fileName):
    # averaging sentiment score
    positive, negative, neutral, mixed = 0, 0, 0, 0

    for score in response["ResultList"]:
        positive += score["SentimentScore"]["Positive"]
        negative += score["SentimentScore"]["Negative"]
        neutral += score["SentimentScore"]["Neutral"]
        mixed += score["SentimentScore"]["Mixed"]

    total_record = len(response["ResultList"])

    mapping = {
        "POSITIVE": positive / total_record,
        "NEGATIVE": negative / total_record,
        "NEUTRAL": neutral / total_record,
        "MIXED": mixed / total_record
    }

    response = {
        'jobid': fileName,
        'user': response_type,
        'TimeStamp': time.ctime(),
        'Sentiment': max(mapping, key=mapping.get),
        'SentimentScore': {
            'Positive': mapping["POSITIVE"],
            'Negative': mapping["NEGATIVE"],
            'Neutral': mapping["NEUTRAL"],
            'Mixed': mapping["MIXED"]
        },
        #'timestamp' = datetime.datetime.now().timestamp()
    
    }
    return response
