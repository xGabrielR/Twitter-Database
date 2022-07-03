import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

BEARER_TOKEN = os.getenv('BEARER_TOKEN')
CONSUMER_KEY = os.getenv('CONSUMER_KEY')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

MINIO_HOST = os.getenv('MINIO_HOST')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')