import os
import time
import json
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import storage

CURRENT_DATE_TIME = datetime.now().isoformat()

# Push Airflow Variables
def push_airflow_variables(**context):
    API_KEY = Variable.get("AIRFLOW_YOUTUBE_API_KEY", default_var=None)
    CHANNEL_ID = Variable.get("AIRFLOW_YOUTUBE_CHANNEL_ID", default_var=None)
    GCP_CREDENTIALS_PATH = Variable.get("GOOGLE_APPLICATION_CREDENTIALS")
    BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")

    if not API_KEY or not CHANNEL_ID or not GCP_CREDENTIALS_PATH or not BUCKET_NAME:
        raise ValueError("Missing one or more Airflow Variables")

    ti = context['ti']
    ti.xcom_push(key='api_key', value=API_KEY)
    ti.xcom_push(key='channel_id', value=CHANNEL_ID)
    ti.xcom_push(key='gcp_credentials_path', value=GCP_CREDENTIALS_PATH)
    ti.xcom_push(key='bucket_name', value=BUCKET_NAME)

# Get Channel Info
def get_channel_info(**context):
    ti = context['ti']
    api_key = ti.xcom_pull(task_ids='push_airflow_variables', key='api_key')
    channel_id = ti.xcom_pull(task_ids='push_airflow_variables', key='channel_id')

    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "snippet,statistics,contentDetails",
        "id": channel_id,
        "key": api_key
    }
    response = requests.get(url, params=params)
    data = response.json()
    if "items" in data:
        channel_info = data["items"][0]
        channel_info["fetched_at"] = CURRENT_DATE_TIME
        ti.xcom_push(key='channel_info', value=channel_info)
    else:
        raise Exception(f"No channel info found: {data}")

# Get All Playlists
def get_playlists(**context):
    ti = context['ti']
    api_key = ti.xcom_pull(task_ids='push_airflow_variables', key='api_key')
    channel_id = ti.xcom_pull(task_ids='push_airflow_variables', key='channel_id')

    url = "https://www.googleapis.com/youtube/v3/playlists"
    playlists = []
    next_page_token = None

    while True:
        params = {
            "part": "snippet,contentDetails",
            "channelId": channel_id,
            "maxResults": 50,
            "key": api_key,
            "pageToken": next_page_token
        }
        res = requests.get(url, params=params)
        data = res.json()
        items = data.get("items", [])
        for item in items:
            item["fetched_at"] = CURRENT_DATE_TIME
        playlists.extend(items)
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
        time.sleep(0.1)

    ti.xcom_push(key='playlists', value=playlists)

# Get Videos from First Playlist
def get_videos_from_playlist(**context):
    ti = context['ti']
    api_key = ti.xcom_pull(task_ids='push_airflow_variables', key='api_key')
    playlists = ti.xcom_pull(task_ids='get_playlists', key='playlists')

    if not playlists:
        raise ValueError("No playlists found")

    first_playlist_id = playlists[0]['id']
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    videos = []
    next_page_token = None

    while True:
        params = {
            "part": "snippet",
            "playlistId": first_playlist_id,
            "maxResults": 50,
            "key": api_key,
            "pageToken": next_page_token
        }
        res = requests.get(url, params=params)
        data = res.json()
        items = data.get("items", [])
        for item in items:
            item["fetched_at"] = CURRENT_DATE_TIME
        videos.extend(items)
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break
        time.sleep(0.1)

    ti.xcom_push(key='videos', value=videos)

# Get Video Stats
def get_video_stats(**context):
    ti = context['ti']
    api_key = ti.xcom_pull(task_ids='push_airflow_variables', key='api_key')
    videos = ti.xcom_pull(task_ids='get_videos_from_playlist', key='videos')

    video_ids = [v['snippet']['resourceId']['videoId'] for v in videos if 'resourceId' in v['snippet']]
    url = "https://www.googleapis.com/youtube/v3/videos"
    video_data = []

    def chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    for chunk in chunked(video_ids, 50):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(chunk),
            "key": api_key
        }
        res = requests.get(url, params=params)
        data = res.json()
        for item in data.get("items", []):
            item["fetched_at"] = CURRENT_DATE_TIME
            video_data.append(item)
        time.sleep(0.1)

    ti.xcom_push(key='video_stats', value=video_data)

# Upload to GCS (including local files)
def upload_to_gcs(**context):
    ti = context['ti']
    gcp_credentials_path = ti.xcom_pull(task_ids='push_airflow_variables', key='gcp_credentials_path')
    bucket_name = ti.xcom_pull(task_ids='push_airflow_variables', key='bucket_name')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_credentials_path

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Use a detailed datetime for folder naming, e.g., 2025-06-29-153045
    current_datetime = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    base_path = f'raw_data_layer/{current_datetime}/'

    data = {
        "channel_info": ti.xcom_pull(task_ids='get_channel_info', key='channel_info'),
        "playlists": ti.xcom_pull(task_ids='get_playlists', key='playlists'),
        "videos": ti.xcom_pull(task_ids='get_videos_from_playlist', key='videos'),
        "video_stats": ti.xcom_pull(task_ids='get_video_stats', key='video_stats'),
    }

    # Upload API response data under raw_data_layer/{datetime}/
    for key, value in data.items():
        blob = bucket.blob(f'{base_path}{key}.json')
        blob.upload_from_string(
            data=json.dumps(value, indent=2),
            content_type='application/json'
        )
        print(f"Uploaded {key} to GCS as {blob.name}")

    # Upload local files under raw_data_layer/{datetime}/files/
    local_files_dir = "/opt/airflow/data"
    if os.path.exists(local_files_dir):
        for file_name in os.listdir(local_files_dir):
            local_file_path = os.path.join(local_files_dir, file_name)
            if os.path.isfile(local_file_path):
                blob = bucket.blob(f'{base_path}files/{file_name}')
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded local file {file_name} to GCS as {blob.name}")


# Define DAG
with DAG(
    dag_id='youtubeapi_to_gcs',
    description='YouTube API data pipeline with GCS upload and local file sync',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    tags=['youtube', 'gcs', 'xcom'],
) as dag:

    t1 = PythonOperator(
        task_id='push_airflow_variables',
        python_callable=push_airflow_variables,
    )

    t2 = PythonOperator(
        task_id='get_channel_info',
        python_callable=get_channel_info,
    )

    t3 = PythonOperator(
        task_id='get_playlists',
        python_callable=get_playlists,
    )

    t4 = PythonOperator(
        task_id='get_videos_from_playlist',
        python_callable=get_videos_from_playlist,
    )

    t5 = PythonOperator(
        task_id='get_video_stats',
        python_callable=get_video_stats,
    )

    t6 = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
    )

    # Task Dependencies
    t1 >> [t2, t3]
    t3 >> t4 >> t5 >> t6
    
