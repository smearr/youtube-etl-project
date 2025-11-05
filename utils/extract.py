import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
REGIONS = ["US", "CA", "GB", "AU", "IN"]

def get_youtube_service():
    """Build YouTube service with API key (no ADC needed)."""
    if not API_KEY:
        raise ValueError("YOUTUBE_API_KEY not found in environment variables!")
    
    return build('youtube', 'v3', developerKey=API_KEY)

def fetch_trending_videos(region_code, max_results=50):
    """Fetch trending videos for a region."""
    try:
        youtube = get_youtube_service()
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=region_code,
            maxResults=max_results
        )
        response = request.execute()
        return response.get("items", [])
    except HttpError as e:
        print(f"An error occurred fetching videos for {region_code}: {e}")
        return []

def save_raw_json(data, region_code):
    """Save raw JSON data to file."""
    import json
    from datetime import datetime
    
    os.makedirs("raw_data", exist_ok=True)
    today = datetime.today().strftime("%Y-%m-%d")
    path = f"raw_data/trending_{region_code}_{today}.json"
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    print(f"✅ Saved raw data to {path}")
    return path
