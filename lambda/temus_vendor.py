import json
import boto3
import random
import pandas as pd

kinesis_client = boto3.client('kinesis', region_name='eu-north-1')
# Constants:
STREAM_NAME = "temus-vendor-data-staging"
VENDOR_DATA_URL = "https://temus-northstar.github.io/data_engineering_case_study_public/vendor_data.html"

def lambda_handler(event, context):
    processed = 0
    print(STREAM_NAME)
    try:
        df_vendor = pd.read_html(VENDOR_DATA_URL)[0]

        # remove spaces in column names
        df_vendor.columns = df_vendor.columns.str.replace(' ', '_')

        push_to_kinesis = []
        record_count = 0
        # Convert to put_records compatible format
        for _, record in df_vendor.T.to_dict().items():
            push_to_kinesis.append(
                {
                    'Data': json.dumps(record),
                    'PartitionKey': str(random.randint(10, 100))
                }
            )
            record_count += 1
            if record_count >= 500:
                kinesis_client.put_records(
                    Records=push_to_kinesis, StreamName=STREAM_NAME
                )
                processed += record_count
                record_count = 0
        if record_count < 500:
            kinesis_client.put_records(
                Records=push_to_kinesis, StreamName=STREAM_NAME
            )
            processed += record_count
        
    except Exception as e:
        print(e)
    message = f'Successfully processed {processed} events.'
    return {
        'statusCode': 200,
        'body': { 'lambdaResult': message }
    }
