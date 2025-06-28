import os
import time
import json
import requests
from datetime import datetime
from pprint import pprint
from dotenv import load_dotenv

# ---------------------------- Load Environment Variables ----------------------------
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")
CURRENT_DATE_TIME = datetime.now().isoformat()

# GET CHANNEL DETAILS
def get_channel_info(api_key, channel_id):
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
        return channel_info
    else:
        raise Exception(f"No channel info found: {data}")

# GET ALL PLAYLISTS
def get_playlists(api_key, channel_id):
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
    return playlists

# GET VIDEOS FROM PLAYLIST
def get_videos_from_playlist(api_key, playlist_id):
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    videos = []
    next_page_token = None

    while True:
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
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
    return videos

# GET VIDEO STATISTICS
def get_video_stats(api_key, video_ids):
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

    return video_data

# MAIN LOGIC
if __name__ == "__main__":
    if not API_KEY:
        print("Missing YOUTUBE_API_KEY in .env")
        exit(1)

    # Channel Metadata
    channel_info = get_channel_info(API_KEY, CHANNEL_ID)
    pprint(f"Channel Info:\n{channel_info['snippet']['title']} - {channel_info['statistics']}")

    # Playlists
    playlists = get_playlists(API_KEY, CHANNEL_ID)
    print(f"Found {len(playlists)} playlists.")

    all_videos = {}
    video_ids_set = set()

    for playlist in playlists:
        playlist_id = playlist["id"]
        title = playlist["snippet"]["title"]
        print(f"Playlist: {title} ({playlist_id})")

        playlist_videos = get_videos_from_playlist(API_KEY, playlist_id)

        video_ids = [
            item["snippet"]["resourceId"]["videoId"]
            for item in playlist_videos
            if "resourceId" in item["snippet"]
        ]

        for vid in video_ids:
            video_ids_set.add(vid)

        all_videos[playlist_id] = {
            "playlist_title": title,
            "video_ids": video_ids,
            "fetched_at": CURRENT_DATE_TIME
        }

    print(f"Unique Videos Collected: {len(video_ids_set)}")

    # Video-Level Data
    video_details = get_video_stats(API_KEY, list(video_ids_set))
    print(f"Video Metadata Retrieved: {len(video_details)}")

    # Save to JSON files
    os.makedirs("data", exist_ok=True)

    with open("data/channel_info.json", "w", encoding="utf-8") as f:
        json.dump(channel_info, f, indent=2)

    with open("data/playlists.json", "w", encoding="utf-8") as f:
        json.dump(playlists, f, indent=2)

    with open("data/playlist_videos.json", "w", encoding="utf-8") as f:
        json.dump(all_videos, f, indent=2)

    with open("data/video_details.json", "w", encoding="utf-8") as f:
        json.dump(video_details, f, indent=2)

    # Preview one video
    if video_details:
        print("Sample Video Data:")
        pprint(video_details[0])