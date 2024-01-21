'''This module contains fundamental parts of the ETL pipeline.'''

from src.APIConnector import APIConnector
from dotenv import load_dotenv
import os
import pandas as pd
import re
import datetime
import logging
import sqlalchemy

load_dotenv()

logging.basicConfig(filename='etl.log', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

def extract(channels: list):

    logging.info(f'Requested data from the following channels: {",".join(channels)}')

    yt_api_connector = APIConnector(api_key=os.getenv('API_KEY'))
    yt_client = yt_api_connector.create_api_connection()

    channels_data_list, videos_data_list = [], []
    channel_to_video_dict = {'channel_id': [], 'video_id': []}

    for channel_name in channels:
        logging.info(f'Trying to fetch channel data for: {channel_name}')
        channel_data = yt_api_connector.request_channel_data(yt_client,parts="contentDetails,statistics,snippet",channel_name=channel_name)['items'][0]

        # Extract channel data to a dataframe
        channel_data_dict = {
            'id': channel_data['id'],
            'name': channel_name,
            'published_at': channel_data['snippet'].get('publishedAt',None),
            'video_count': channel_data['statistics'].get('videoCount',None),
            'subscriber_count': channel_data['statistics'].get('subscriberCount',None),
            'view_count': channel_data['statistics'].get('viewCount',None)
        }

        channels_data_list.append(channel_data_dict)

        logging.info('Channel data fetched successfully')

        # Extract video data to a dataframe
        channel_full_playlist_id = channel_data['contentDetails']['relatedPlaylists']['uploads']
        videos_ids = yt_api_connector.request_list_of_channel_videos(yt_client,channel_full_playlist_id)

        # Youtube API doesn't allow to use pageToken parameter in conjunction with video id parameter
        # and the default maximum number of results per one page is 50. In order to minimize the number of
        # API calls, divide the list of video ids into 50-element chunks and retrieve data on their basis

        n_intervals = len(videos_ids) // 50
        
        for i in range(n_intervals+1):
            video_ids_interval = videos_ids[i*50:(i+1)*50]

            video_data = yt_api_connector.request_video_data(yt_client,parts='contentDetails,statistics,snippet',video_id=video_ids_interval)['items']
            for video in video_data:
                # Calculate video duration
                video_duration = video['contentDetails']['duration']
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

                video_data_dict = {
                    'id': video['id'],
                    'title': video['snippet'].get('title',None),
                    'published_at': video['snippet'].get('publishedAt',None),
                    'duration': int(total_duration),
                    'view_count': video['statistics'].get('viewCount',None),
                    'like_count': video['statistics'].get('likeCount',None),
                    'comment_count': video['statistics'].get('commentCount',None)
                }

                videos_data_list.append(video_data_dict)

        # Save the information about channel-video pairs to a dataframe
        logging.debug(f'Number of videos on {channel_name}: {channel_data_dict["video_count"]}')
        logging.debug(f'Number of retrieved video ids from {channel_name}: {len(videos_ids)}')

        channel_to_video_dict['channel_id'].extend([channel_data_dict['id']] * int(channel_data_dict['video_count']))
        channel_to_video_dict['video_id'].extend(videos_ids)

    # Create a dataframes 
    channels_df = pd.DataFrame(channels_data_list)
    videos_df = pd.DataFrame(videos_data_list)
    channel_to_video_df = pd.DataFrame(channel_to_video_dict)

    # Convert publication dates to YYYY-MM-DD
    channels_df['published_at'] = pd.to_datetime(channels_df['published_at']).dt.strftime('%Y-%m-%d')
    videos_df['published_at'] = pd.to_datetime(videos_df['published_at']).dt.strftime('%Y-%m-%d')

    # Insert the data into mysql database
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{os.getenv("MYSQL_USER")}:{os.getenv("MYSQL_PASSWORD")}@{os.getenv("MYSQL_HOST")}:3306/{os.getenv("MYSQL_DB")}')
    
    channels_df.to_sql('channel_data', con=engine, index=False, if_exists='replace')
    videos_df.to_sql('video_data', con=engine, index=False, if_exists='replace')
    channel_to_video_df.to_sql('channel_to_video', con=engine, index=False, if_exists='replace')

    return channels_df, videos_df, channel_to_video_df

def transform():
    pass

def load():
    pass