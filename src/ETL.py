'''This module contains fundamental parts of the ETL pipeline.'''

from src.APIConnector import APIConnector
from dotenv import load_dotenv
import os
import pandas as pd
import re
import datetime

load_dotenv()

def extract(channels: list):
    yt_api_connector = APIConnector(api_key=os.getenv('API_KEY'))
    yt_client = yt_api_connector.create_api_connection()

    channels_data_list, videos_data_list = [], []
    channel_to_video_dict = {'channel_id': [], 'video_id': []}

    # Iterate over the channels and extract the channel data and video data
    for channel_name in channels:
        channel_data = yt_api_connector.request_channel_data(yt_client,parts="contentDetails,statistics,snippet",channel_name=channel_name)['items'][0]

        # Extract channel data to a dataframe
        channel_data_dict = {
            'id': channel_data['id'],
            'name': channel_name,
            'published_at': channel_data['snippet']['publishedAt'],
            'video_count': int(channel_data['statistics']['videoCount']),
            'subscriber_count': int(channel_data['statistics']['subscriberCount']),
            'view_count': int(channel_data['statistics']['viewCount'])
        }

        channels_data_list.append(channel_data_dict)

        # Extract video data to a dataframe
        channel_full_playlist_id = channel_data['contentDetails']['relatedPlaylists']['uploads']
        videos_ids = yt_api_connector.request_list_of_channel_videos(yt_client,channel_full_playlist_id)
        videos_data = yt_api_connector.request_video_data(yt_client,parts='contentDetails,statistics,snippet',video_id=videos_ids)['items']

        for video in videos_data:
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
                'title': video['snippet']['title'],
                'date_published': video['snippet']['publishedAt'],
                'duration': int(total_duration),
                'view_count': int(video['statistics']['viewCount']),
                'like_count': int(video['statistics']['likeCount']),
                'comment_count': int(video['statistics']['commentCount'])
            }

            videos_data_list.append(video_data_dict)

        # Save the information about channel-video pairs to a dataframe
        channel_to_video_dict['channel_id'].extend([channel_data_dict['id']] * int(channel_data_dict['video_count']))
        channel_to_video_dict['video_id'].extend(videos_ids)

    # Perform initial cleaning (date formatting, number/string formatting)

    # Create a dataframe and insert the data into mysql database
    channels_df = pd.DataFrame(channels_data_list)
    videos_df = pd.DataFrame(videos_data_list)
    # channel_to_video_df = pd.DataFrame(channel_to_video_dict)

    return channels_df, videos_df

def transform():
    pass

def load():
    pass