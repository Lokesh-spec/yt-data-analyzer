import os
import requests
from dotenv import load_dotenv
from pprint import pprint


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv('YOUTUBE_API_KEY')
    CHANNEL_ID = "UCH26LcOJOd31yw0gIpBJFog"

    channel_url = "https://www.googleapis.com/youtube/v3/channels"

    channel_params = {
        "part" : "contentDetails",
        "id" : CHANNEL_ID,
        "key" : API_KEY
    }

    response = requests.get(channel_url, channel_params)
    channel_data = response.json()

    uploads_playlist_id = channel_data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # Get Video IDs from Playlist
    playlist_url = "https://www.googleapis.com/youtube/v3/playlistItems"
    video_ids = []
    next_page_token = None

    while True:
        playlist_params = {
            "part": "snippet",
            "playlistId": uploads_playlist_id,
            "maxResults": 50,  
            "key": API_KEY,
            "pageToken": next_page_token
        }

        pl_response = requests.get(playlist_url, params=playlist_params)
        pl_data = pl_response.json()

        for item in pl_data["items"]:
            video_ids.append(item["snippet"]["resourceId"]["videoId"])

        next_page_token = pl_data.get("nextPageToken")
        if not next_page_token:
            break
    
    for video_id in video_ids:
        print(video_id)

    print(len(video_ids))
    
    # comments_thread_url = "https://www.googleapis.com/youtube/v3/commentThreads"

    # for video_id in video_ids:
    #     comments_thread_params = {
    #         "part" : "snippet,replies",
    #         "videoId" : video_id,
    #         "maxRecords" : 50,
    #         "key" : API_KEY
    #     }
    #     comments_thread_response = requests.get(comments_thread_url, params=comments_thread_params)
    #     comments_thread_data = comments_thread_response.json()

    #     pprint(comments_thread_data)
    #     break
    # video_url = "https://www.googleapis.com/youtube/v3/videos"

    # for video_id in video_ids:
    
    #     video_params = {
    #         "part": "snippet,statistics",
    #         "id": video_id,
    #         "key": API_KEY
    #     }

    #     video_response = requests.get(video_url, params=video_params)
    #     video_data = video_response.json()

    #     # print(video_data)

    #     # Print Results
    #     for video in video_data["items"]:
    #         title = video["snippet"]["title"]
    #         views = video["statistics"].get("viewCount", "N/A")
    #         likes = video["statistics"].get("likeCount", "N/A")
    #         comments = video["statistics"].get("commentCount", "N/A")

    #         print(f"Title: {title}")
    #         print(f"Views: {views}")
    #         print(f"Likes: {likes}")
    #         print(f"Comments: {comments}")
    #         print("-" * 40)
