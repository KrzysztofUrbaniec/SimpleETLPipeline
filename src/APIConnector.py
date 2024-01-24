'''This module defines an API connector allowing to create connection to YT API and retrieve data in json format.'''

import googleapiclient.discovery

class APIConnector:
    '''Convenience class for YT data retrieval.'''

    def __init__(self,api_key):
        self.api_key = api_key

    def create_api_connection(self) -> googleapiclient.discovery.Resource:
        '''Create and return Youtube API v3 client'''

        api_service_name = "youtube"
        api_version = "v3"
        yt_api_key = self.api_key
        
        self.client = googleapiclient.discovery.build(api_service_name,
                                                 api_version, 
                                                 developerKey=yt_api_key)

    def request_channel_data(self, parts: str, id: str) -> dict:
        '''Request channel data. Parts should be a coma-separated string of channel resource properties, like: brandingDetails,snippet,statistics.
        For more detailed description visit official documentation: https://developers.google.com/youtube/v3/docs/channels/list.
        
        Returns data in json format.'''

        response = self.client.channels().list(part=parts,
                                          id=id
                                          ).execute()
        return response

    def request_video_data(self, parts: str, video_id: list) -> dict:
        '''Request video data. Parts should be a coma-separated string of channel resource properties, like: brandingDetails,snippet,statistics.
        For more detailed description visit official documentation: https://developers.google.com/youtube/v3/docs/videos/list.
        
        Returns data in json format.'''

        response = self.client.videos().list(part=parts,
                                        id=video_id
                                        ).execute()
        return response
    
    def request_list_of_channel_videos(self, playlist_id: str) -> list:
        '''Request channel data. Parts should be a coma-separated string of channel resource properties, like: brandingDetails,snippet,statistics.
        Playlist_id should correspond to the playlist of all videos on the channel. 
        This id can be retrieved from channel data in contentDetails -> relatedPlaylists -> uploads section of json data.
        For more detailed description visit official documentation: https://developers.google.com/youtube/v3/docs/playlists/list.
        
        Returns a list of video ids.'''

        next_page_token = None
        video_ids = []

        while True:
            response = self.client.playlistItems().list(part='contentDetails',
                                                playlistId=playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token
                                                ).execute()
            
            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                video_info = self.client.videos().list(part='status', id=video_id).execute()['items'][0]['status']

                # Check if the video status is processed
                if video_info['uploadStatus'] == 'processed':
                    video_ids.append(video_id)

            next_page_token = response.get('nextPageToken')

            # If next_page_token is None (response contains no more pages), stop
            if not next_page_token:
                break

        return video_ids