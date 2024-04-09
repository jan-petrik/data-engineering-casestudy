import json
import boto3
import random
import pandas as pd
import numpy as np

kinesis_client = boto3.client('kinesis', region_name='eu-north-1')
# Constants:
STREAM_NAME = "temus-product-data-staging"
PRODUCT_DATA_URL = "https://temus-northstar.github.io/data_engineering_case_study_public/product_data.html"

def _randomize_data(df, thresh_price: float = 0.15, thresh_stock: float = 0.05):
    df["prob_price"] = np.random.rand(len(df), 1)
    df["Sale_Price"] = np.where(df["prob_price"] <= thresh_price, df["Sale_Price"]*(1 + df["prob_price"]), df["Sale_Price"])

    df["prob_stock"] = np.random.rand(len(df), 1)
    df["Stock_Status"] = np.where(df["prob_stock"] <= thresh_stock, "Low Stock", df["Stock_Status"])
    df["Stock_Status"] = np.where(df["prob_stock"] <= thresh_stock/2, "Out of Stock", df["Stock_Status"])

    df.drop(columns=["prob_price", "prob_stock"], inplace=True)

    return df

def lambda_handler(event, context):
    processed = 0
    print(STREAM_NAME)
    try:
        df_product = pd.read_html(PRODUCT_DATA_URL)[0]

        # replace spaces in column names
        df_product.columns = df_product.columns.str.replace(' ', '_')

        df_product = _randomize_data(df=df_product)

        push_to_kinesis = []
        record_count = 0
        # Convert to put_records compatible format
        for _, record in df_product.T.to_dict().items():
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
                push_to_kinesis = []
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
