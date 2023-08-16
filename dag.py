

from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
import pandas as pd
from newsapi import NewsApiClient
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import requests
import ast

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
                "apiKey": "pub_276036b4c583f45f88de54dfb6e1cd240fb0b",
                "q": "tesla",
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
            newsapi = NewsApiClient(api_key="f7480f77896f4fb58d6e7b2ba0172c04")

            today = datetime.today().date()

            days_ago = today - timedelta(days=28)

            days_ago = days_ago.strftime('%Y-%m-%d')

            newsapi_articles = newsapi.get_everything(q='tesla',
                                            sources='bbc-news,the-verge',
                                            domains='bbc.co.uk,techcrunch.com',
                                            from_param=days_ago,
                                            language='en',
                                            sort_by='relevancy')

            return newsapi_articles


        na_df = pd.DataFrame(get_data_from_newsapi_api()["articles"])
        nd_df = pd.DataFrame(get_data_from_newsdata_api()["results"])

        # Transform into correct json and get the source only from newsApi
        def extract_name(row):
            
            return row['name']

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
                # Format news data pubDate into datetime
        def format_nd_df_datetime(row):
            timestamp = datetime.strptime(row, "%Y-%m-%d %H:%M:%S")   
            timestamp_seconds = timestamp.timestamp()
            dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ") 

        nd_df["pubDate"] = nd_df["pubDate"].apply(format_nd_df_datetime)

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

        final_data_frame = final_data_frame.iloc[:, :-6]

        final_data_frame.to_csv('final_dataframe.csv')
        return 'final_dataframe.csv'    

    upload_task = LocalFilesystemToS3Operator(
        task_id="Upload-to-S3",
        aws_conn_id='aws_connection',
        filename='/home/iloreus/airflow/dags/newsproject/final_dataframe.csv',
        dest_bucket='news-collectors',
        dest_key=datetime.today().date().strftime('%Y/%m/%d') + '.csv'
    )

       
    merge_dataframes() >> upload_task



my_dag_instance = news_collectors()