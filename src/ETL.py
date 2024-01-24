'''This module contains fundamental parts of the ETL pipeline.'''

from src.APIConnector import APIConnector
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
import re
import datetime
import logging
import sqlalchemy

load_dotenv()

logging.basicConfig(level=logging.INFO, filename='etl.log', filemode='w')

def calculate_duration(video_duration):
    hour_pattern = re.compile(r'(\d+)H')
    minute_pattern = re.compile(r'(\d+)M')
    second_pattern = re.compile(r'(\d+)S')

    hours = hour_pattern.search(video_duration)
    minutes = minute_pattern.search(video_duration)
    seconds = second_pattern.search(video_duration)

    hours = int(hours.group(1)) if hours else 0
    minutes = int(minutes.group(1)) if minutes else 0
    seconds = int(seconds.group(1)) if seconds else 0

    total_duration = datetime.timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    ).total_seconds()

    return int(total_duration)

def fetch_channel_data(api_connector, channel_id):
    logging.info(f'Trying to fetch channel data for: {channel_id}')
    channel_data = api_connector.request_channel_data(parts="contentDetails,statistics,snippet", id=channel_id)

    if 'items' in channel_data and channel_data['items']:

        channel_data = channel_data['items'][0]
        channel_full_playlist_id = channel_data['contentDetails']['relatedPlaylists']['uploads']

        channel_data_dict = {
            'id': channel_data['id'],
            'name': channel_data['snippet']['title'],
            'published_at': channel_data['snippet'].get('publishedAt', None),
            'video_count': channel_data['statistics'].get('videoCount', None),
            'subscriber_count': channel_data['statistics'].get('subscriberCount', None),
            'view_count': channel_data['statistics'].get('viewCount', None)
        }

        return channel_data_dict, channel_full_playlist_id

    else:
        logging.warning(f'Requested channel: {channel_id} has not been found. Proceeding to the next channel.')
        return None, None

def fetch_list_of_videos(api_connector, channel_full_playlist_id):
    videos_ids = api_connector.request_list_of_channel_videos(channel_full_playlist_id)
    return videos_ids

def fetch_video_details(api_connector, video_ids):
    video_data_list = []

    for video_id in video_ids:
        logging.info(f'Trying to fetch video with id: {video_id}')
        video_data = api_connector.request_video_data(parts='contentDetails,statistics,snippet', video_id=video_id)['items'][0]

        total_duration = calculate_duration(video_data['contentDetails']['duration'])

        video_data_dict = {
            'id': video_data['id'],
            'title': video_data['snippet'].get('title', None),
            'published_at': video_data['snippet'].get('publishedAt', None),
            'duration': total_duration,
            'view_count': video_data['statistics'].get('viewCount', None),
            'like_count': video_data['statistics'].get('likeCount', None),
            'comment_count': video_data['statistics'].get('commentCount', None)
        }

        video_data_list.append(video_data_dict)

    return video_data_list

def save_to_database(channels_df, videos_df, channel_to_video_df):
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@{os.getenv("MYSQL_HOST")}:3306/{os.getenv("MYSQL_DB")}')
    
    channels_df.to_sql('channel_data', con=engine, index=False, if_exists='replace')
    videos_df.to_sql('video_data', con=engine, index=False, if_exists='replace')
    channel_to_video_df.to_sql('channel_to_video', con=engine, index=False, if_exists='replace')

def extract(channels: list):
    logging.info(f'Requested data from the following channels: {",".join(channels)}')

    yt_api_connector = APIConnector(api_key=os.getenv('API_KEY'))
    yt_api_connector.create_api_connection()

    channels_data_list, videos_data_list = [], []
    channel_to_video_dict = {'channel_id': [], 'video_id': []}

    for channel_id in channels:
        channel_data_dict, channel_full_playlist_id = fetch_channel_data(yt_api_connector, channel_id)

        if channel_data_dict is not None:
            channels_data_list.append(channel_data_dict)

            video_ids = fetch_list_of_videos(yt_api_connector, channel_full_playlist_id)

            videos_data_list.extend(fetch_video_details(yt_api_connector, video_ids))

            logging.info(f"channel_id * video_count: {len([channel_data_dict['id']] * int(channel_data_dict['video_count']))}")
            logging.info(f"Number of video ids: {len(video_ids)}")

            channel_to_video_dict['channel_id'].extend([channel_data_dict['id']] * int(channel_data_dict['video_count']))
            channel_to_video_dict['video_id'].extend(video_ids)

    if len(channels_data_list) > 0:
        # Create dataframes 
        channels_df = pd.DataFrame(channels_data_list)
        videos_df = pd.DataFrame(videos_data_list)
        channel_to_video_df = pd.DataFrame(channel_to_video_dict)

        # Convert publication dates to YYYY-MM-DD
        channels_df['published_at'] = pd.to_datetime(channels_df['published_at']).dt.strftime('%Y-%m-%d')
        videos_df['published_at'] = pd.to_datetime(videos_df['published_at']).dt.strftime('%Y-%m-%d')

        save_to_database(channels_df, videos_df, channel_to_video_df)

        return channels_df, videos_df, channel_to_video_df
    
def transform(channels_df, videos_df, channel_to_video_df):
   
    # Calculate engagement rate 
    videos_df['view_count'] = videos_df['view_count'].astype(int)
    videos_df['engagement_rate'] = ((videos_df['like_count'].astype(int) + videos_df['comment_count'].astype(int)) /  videos_df['view_count'].astype(int)).round(3)

    # Extract year and month from the date
    videos_df['year'] = pd.to_datetime(videos_df['published_at']).dt.year
    videos_df['month'] = pd.to_datetime(videos_df['published_at']).dt.month

    # Merge dataframes for easier manipulation
    merged_df = pd.merge(channels_df, channel_to_video_df, left_on='id', right_on='channel_id', how='inner', suffixes=('_channel','_channel_to_video'))
    merged_df = pd.merge(merged_df, videos_df, left_on='video_id', right_on='id', how='inner', suffixes=('_channel_to_video','_video'))

    # Group by year and month and calculate total view count per month and average engagement per video per month
    print(merged_df.columns)
    final_df = merged_df.groupby(['name','year','month']) \
                        .agg({'video_id':'count','view_count_video':'sum', 'engagement_rate':'mean'}) \
                        .sort_values(by=['year','month'],ascending=[False,False]) \
                        .reset_index()
    final_df.rename({'video_id':'number_of_videos', 'view_count_video':'total_view_count'}, axis=1, inplace=True)

    return final_df

def load(transformed_df, table_name):
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@{os.getenv("MYSQL_HOST")}:3306/{os.getenv("MYSQL_DB")}')
    transformed_df.to_sql(table_name, con=engine, index=False, if_exists='replace')