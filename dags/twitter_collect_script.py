import os
import json
import time
import pandas as pd

from minio import Minio
from datetime import datetime
from tweepy import OAuthHandler, API, Client
from utils import MINIO_HOST,ACCESS_KEY,SECRET_KEY,MINIO_ENDPOINT,BasePipeline
from utils import CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET,BEARER_TOKEN

from pyspark.sql import SparkSession
from pyspark.sql import functions as psf

SEARCH_TIMES = 100
SEARCH_TERMS = 'Brasil'

DATE = datetime.now().strftime('%Y-%m-%d')
COLLECT_DATETIME = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


minio_client = Minio(
    endpoint=MINIO_HOST,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

spark = (
    SparkSession
    .builder
    .appName('pipeline')
    .config('fs.s3a.endpoint', MINIO_ENDPOINT)
    .config('fs.s3a.buffer.dir', 's3a://twitterlake')
    .config('fs.s3a.fast.upload.buffer', 'bytebuffer')
    .config('fs.s3a.fast.upload.active.blocks', 1)
    .config('fs.s3a.access.key', 'minioadmin')
    .config('fs.s3a.secret.key', 'minioadmin')
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.path.style.access", "True")
    #.config('spark.jars', os.path.join(JARS_DIR, 'postgresql-42.3.2.jar')) # Set Postgres Jars
    .getOrCreate()
)

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = API(auth)
twitter_client = Client(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET,
                        access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET,
                        bearer_token=BEARER_TOKEN)


class TwitterRequests(BasePipeline):
    def __init__(self):
        self.airflow_filepath = '/opt/airflow/data/data_backup'
        self.post_cols = ['id', 'user_id', 'geo', 'text', 'place', 'source', 'coordinates', 'contributors', 'retweet_count', 'fav_count', 'is_quote', 'urls']
        self.user_cols = ['id', 'name', 'lang', 'is_verified', 'location', 'time_zone', 'created_at', 'description', 'screen_name', 'geo_enabled', 'friends_count', 'profile_color', 'followers_count', 'favourites_count', 'background_color']
        self.retweets_info_cols = ['post_id', 'retweet_user_id', 'lang', 'is_fav', 'source', 'fav_counts', 'is_retweet', 'created_at', 'retweet_count']

    def create_buckets(self, *args):
        for bucket in args:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)

        return -1

    def api_request(self):
        tweets = twitter_client.search_recent_tweets(query=SEARCH_TERMS, max_results=SEARCH_TIMES)

        try:
            for k in tweets.data:
                tweet_data = api.get_status(k.id, tweet_mode='extended')._json
                time.sleep(2)
                print(json.dumps(tweet_data) + '\n', file=open(f'/opt/airflow/data/outputs/output_json_tweets_{DATE}.txt', 'a'))

                return 'api_success_request_all_data'

        except ValueError:
            for k in tweet_data:
                print(json.dumps(tweet_data) + '\n', file=open(f'/opt/airflow/data/outputs/output_json_error_{DATE}.txt', 'a'))

            print(f'\n[ API Twitter Request Daily Data ] [{DATE}] -> ERROR Collect Full Data {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 
                  file=open('/opt/airflow/logs/TwitterLogs/time_logs.txt', 'a'))

            return 'api_error_request_all_data'


    def load_data(self, **context):
        if os.path.exists('output_json_error_{DATE}.txt'):
            with open(f'/opt/airflow/data/outputs/output_json_error_{DATE}.txt', 'r') as file_jsons:
                data = file_jsons.readlines()

            data_jsons = [json.loads(data[k]) for k in range(0, len(data), 2)]
            data_jsons = json.dumps(data_jsons)
            context['ti'].xcom_push(key='data_jsons', value=data_jsons)

        else:
            with open(f'/opt/airflow/data/outputs/output_json_tweets_{DATE}.txt', 'r') as file_jsons:
                data = file_jsons.readlines()

            data_jsons = [json.loads(data[k]) for k in range(0, len(data), 2)]
            data_jsons = json.dumps(data_jsons)
            context['ti'].xcom_push(key='data_jsons', value=data_jsons)

    def get_users(self, **context):

        dataset_users = pd.DataFrame(columns=self.user_cols)

        data_jsons = json.loads(context['ti'].xcom_pull(key='data_jsons'))

        for k in range(len(data_jsons)):
            try:
                post_author_info = data_jsons[k]['retweeted_status']
                has_url = data_jsons[k]['retweeted_status']['entities']['urls']

            except:
                post_autor_info = data_jsons[k]['user']
                data_jsons[k]['entities']['urls']

            post_urls = [url['url'] for url in has_url]

            if post_urls: post_urls = ', '.join([url['url'] for url in has_url])
            else: post_urls = 'None'

            post_user_id = post_author_info['user']['id_str']
            post_user_name = post_author_info['user']['name']
            post_user_lang = post_author_info['user']['lang']
            post_user_verified = post_author_info['user']['verified']
            post_user_location = post_author_info['user']['location']
            post_user_time_zone = post_author_info['user']['time_zone']
            post_user_created_at = post_author_info['user']['created_at']
            post_user_description = post_author_info['user']['description']
            post_user_screen_name = post_author_info['user']['screen_name']

            post_user_geo_enabled = post_author_info['user']['geo_enabled']
            post_user_friends_count = post_author_info['user']['friends_count']
            tweet_user_profile_color = post_author_info['user']['profile_link_color']
            post_user_followers_count = post_author_info['user']['followers_count']
            post_user_favourites_count = post_author_info['user']['favourites_count']
            tweet_user_profile_background_color = post_author_info['user']['profile_background_color']

            user = data_jsons[k]['user'].copy()
            tweet_user_id = user['id_str']
            tweet_user_name = user['name']
            tweet_user_lang = user['lang']
            tweet_user_desc = user['description']
            tweet_user_c_fllw = user['followers_count']
            tweet_user_c_frds = user['friends_count']
            tweet_user_c_fav  = user['favourites_count']
            tweet_user_timezone = user['time_zone']
            tweet_user_location = user['location']
            tweet_user_geo_enab = user['geo_enabled']
            tweet_user_date_acc = user['created_at']
            tweet_user_is_verify = user['verified']
            tweet_user_screen_name = user['screen_name']
            tweet_user_profile_color = user['profile_link_color']
            tweet_user_profile_background_color = user['profile_background_color']

            r_user = [tweet_user_id,tweet_user_name,tweet_user_lang,tweet_user_is_verify,tweet_user_location,tweet_user_timezone,tweet_user_date_acc,tweet_user_desc,tweet_user_screen_name,tweet_user_geo_enab,tweet_user_c_frds,tweet_user_profile_color,
                      tweet_user_c_fllw,tweet_user_c_fav,tweet_user_profile_background_color]
            user = [post_user_id,post_user_name,post_user_lang,post_user_verified,post_user_location,post_user_time_zone,post_user_created_at,post_user_description,post_user_screen_name,
                    post_user_geo_enabled,post_user_friends_count,tweet_user_profile_color,post_user_followers_count,post_user_favourites_count,tweet_user_profile_background_color]
            
            df_user = pd.DataFrame(user).T
            df_user.columns = self.user_cols

            df_r_user = pd.DataFrame(r_user).T
            df_r_user.columns = self.user_cols
            df_user = pd.concat([df_user, df_r_user], axis=0)
            dataset_users = pd.concat([dataset_users, df_user], axis=0)

        dataset_users['request_datetime'] = COLLECT_DATETIME
        dataset_users = dataset_users.reset_index(drop=True)

        if not os.path.exists(f'{self.airflow_filepath}/users'):
            os.makedirs(f'{self.airflow_filepath}/users')

        dataset_users.to_csv(f'{self.airflow_filepath}/users/dataset_users_{DATE}.csv', index=False)

    def get_posts(self, **context):
        dataset_posts = pd.DataFrame(columns=self.post_cols)

        data_jsons = json.loads(context['ti'].xcom_pull(key='data_jsons'))

        for k in range(len(data_jsons)):
            try:
                post_author_info = data_jsons[k]['retweeted_status']
                has_url = data_jsons[k]['retweeted_status']['entities']['urls']

            except:
                post_autor_info = data_jsons[k]['user']
                data_jsons[k]['entities']['urls']

            post_urls = [url['url'] for url in has_url]

            if post_urls: post_urls = ', '.join([url['url'] for url in has_url])
            else: post_urls = 'None'

            try:
                post_place = post_author_info['place']['full_name']
            except:
                post_place = 'None'

            post_id = post_author_info['id_str']
            post_geo = post_author_info['geo']
            post_text = post_author_info['full_text']
            post_source = post_author_info['source']
            post_coordinates = post_author_info['coordinates']
            post_contributors = post_author_info['contributors']
            post_post_author_info_count = post_author_info['retweet_count']
            post_favorite_count = post_author_info['favorite_count']
            post_is_quote_status = post_author_info['is_quote_status']
            post_user_id = post_author_info['user']['id_str']
        
            post = [post_id,post_user_id,post_geo,post_text,post_place,post_source,
                    post_coordinates,post_contributors,post_post_author_info_count,post_favorite_count,post_is_quote_status,post_urls]

            df_post = pd.DataFrame(post).T
            df_post.columns = self.post_cols
            dataset_posts = pd.concat([dataset_posts, df_post], axis=0)

        dataset_posts['request_datetime'] = COLLECT_DATETIME
        dataset_posts = dataset_posts.reset_index(drop=True)

        if not os.path.exists(f'{self.airflow_filepath}/posts'):
            os.makedirs(f'{self.airflow_filepath}/posts')

        dataset_posts.to_csv(f'{self.airflow_filepath}/posts/dataset_posts_{DATE}.csv', index=False)

    def get_retweets(self, **context):
        dataset_retweets = pd.DataFrame(columns=self.retweets_info_cols)

        data_jsons = json.loads(context['ti'].xcom_pull(key='data_jsons'))

        for k in range(len(data_jsons)):

            user = data_jsons[k]['user'].copy()
            tweet_user_id = user['id_str']

            r_post_id = data_jsons[k]['id_str']
            r_post_lang = data_jsons[k]['lang']
            r_post_is_fav = data_jsons[k]['favorited']
            r_post_source = data_jsons[k]['source']
            r_post_fav_count = data_jsons[k]['favorite_count']
            r_post_is_retweet = data_jsons[k]['retweeted']
            r_post_created_at = data_jsons[k]['created_at']
            r_post_retweet_count = data_jsons[k]['retweet_count']


            retweets_info = [r_post_id,tweet_user_id,r_post_lang,r_post_is_fav,r_post_source,
                             r_post_fav_count,r_post_is_retweet,r_post_created_at,r_post_retweet_count]

            df_retweets = pd.DataFrame(retweets_info).T
            df_retweets.columns = self.retweets_info_cols
            dataset_retweets = pd.concat([dataset_retweets, df_retweets], axis=0)

        dataset_retweets['request_datetime'] = COLLECT_DATETIME
        dataset_retweets = dataset_retweets.reset_index(drop=True)

        if not os.path.exists(f'{self.airflow_filepath}/retweets'):
            os.makedirs(f'{self.airflow_filepath}/retweets')

        dataset_retweets.to_csv(f'{self.airflow_filepath}/retweets/dataset_retweets_{DATE}.csv', index=False)
    
    def store_dataset(self):
        dataset_users = spark.read.format('csv').option('header', 'true').load(f'{self.airflow_filepath}/users/dataset_users_{DATE}.csv')
        dataset_posts = spark.read.format('csv').option('header', 'true').load(f'{self.airflow_filepath}/posts/dataset_posts_{DATE}.csv')
        dataset_retweets = spark.read.format('csv').option('header', 'true').load(f'{self.airflow_filepath}/retweets/dataset_retweets_{DATE}.csv')

        dataset_users = dataset_users.withColumn('created_at', psf.date_format(psf.current_timestamp(), 'yyyy_MM_dd-HH_mm_ss'))
        dataset_posts = dataset_posts.withColumn('created_at', psf.date_format(psf.current_timestamp(), 'yyyy_MM_dd-HH_mm_ss'))
        dataset_retweets = dataset_retweets.withColumn('created_at', psf.date_format(psf.current_timestamp(), 'yyyy_MM_dd-HH_mm_ss'))

        try:
            dataset_users.write.format('parquet').mode('overwrite').partitionBy('created_at').save('s3a://twitterlake/spark_users')
            dataset_posts.write.format('parquet').mode('overwrite').partitionBy('created_at').save('s3a://twitterlake/spark_posts')
            dataset_retweets.write.format('parquet').mode('overwrite').partitionBy('created_at').save('s3a://twitterlake/spark_retweets')

        except ValueError:
            print(f'\n[ API Twitter Request Daily Data ] [{DATE}] -> ERROR Spark to S3 at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', 
                  file=open('time_logs.txt', 'a'))

        return None


if __name__ == '__main__':
    HOUR_START = datetime.now().strftime('%H:%M:%S')
    print(f'[ API Twitter Request Daily Data ] [{DATE}] -> Time Start at {HOUR_START}', 
          file=open('/opt/airflow/logs/TwitterLogs/time_logs.txt', 'a'))
    
    tr = TwitterRequests()

    tr.create_buckets('twitterlake')
    
    tr.api_request()
    
    dataset_jsons = tr.load_data()
    
    dfusers = tr.get_users(dataset_jsons)
    dfposts = tr.get_posts(dataset_jsons)
    dfretwe = tr.get_retweets(dataset_jsons)

    tr.store_dataset()

    HOUR_END = datetime.now().strftime('%H:%M:%S')
    print(f'[ API Twitter Request Daily Data ] [{DATE}] -> Time End   at {HOUR_END}\n', file=open('time_logs.txt', 'a'))