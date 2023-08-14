from datetime import datetime
import os
from airflow.decorators import dag, task
from botocore.exceptions import ClientError
import pandas as pd
import boto3
import logging
import json
from newsapi import NewsApiClient
import requests
import ast

nd_api_key = os.getenv("NEWSDATA_API_KEY")
na_api_key = os.getenv("NEWSAPI_API_KEY")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Dags parameters
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 11),
    'retries': 1,
}


@dag(default_args=default_args, schedule_interval='3 * * * *', catchup=False)
def news_collectors():
    @task
    def merge_dataframes():
        def get_data_from_newsdata_api():
            params = {
                "apiKey": nd_api_key,
                "q": "pegasus",
                "language": "en"
            }

            response = requests.get("https://newsdata.io/api/1/news", params=params)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print("Error fetching data. Status code:", response.status_code)
                return None

        def get_data_from_newsapi_api():
            newsapi = NewsApiClient(api_key=na_api_key)

            newsapi_articles = newsapi.get_everything(q='bitcoin',
                                                      sources='bbc-news,the-verge',
                                                      domains='bbc.co.uk,techcrunch.com',
                                                      from_param='2023-07-14',
                                                      language='en',
                                                      sort_by='relevancy')

            return newsapi_articles

        na_df = pd.DataFrame(get_data_from_newsapi_api()["articles"])
        nd_df = pd.DataFrame(get_data_from_newsdata_api()["results"])

        # Transform into correct json and get the source only from newsApi
        def extract_name(row):
            correct_json = row.replace("'", "\"")
            data = json.loads(correct_json)
            return data['name']

        na_df["source"] = na_df["source"].apply(extract_name)

        # Format creator rows for newsData dataframe
        def format_creator(row):
            if row is None or row == 'nan':
                return "Unknown"
            try:
                lists = ast.literal_eval(row)
                return lists[0]
            except (ValueError, SyntaxError):
                return "Unknown"

        nd_df["creator"] = nd_df["creator"].apply(format_creator)

        # Rename all columns for merging
        nd_df.rename(columns={
            "pubDate": "publishedAt",
            "source_id": "source",
            "creator": "author",
            "link": "url",
            "image_url": "urlToImage"
        }, inplace=True)

        # Fill all empty or null fields
        na_df = na_df.fillna("Unknown")
        nd_df = nd_df.fillna("Unknown")

        final_data_frame = pd.merge(na_df, nd_df,
                                    on=['source', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt',
                                        'content'], how='outer')

        final_data_frame.to_csv('final_dataframe.csv')
        final_data_frame = final_data_frame.iloc[:, :-6]

        return "final_dataframe"

    @task
    def upload_to_s3(file_name, bucket):
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

        # If S3 object_name was not specified, use file_name

        object_name = datetime.today().date().strftime('%Y/%m/%d') + '.csv'

        # Upload the file
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    final_df = merge_dataframes()
    upload_task = upload_to_s3(final_df, "news-collectors")

    final_df >> upload_task


my_dag_instance = news_collectors()
