import pandas as pd

def transform_video_data(raw_items):
    rows = []
    for item in raw_items:
        snippet = item["snippet"]
        stats = item["statistics"]
        content = item["contentDetails"]

        rows.append({
            "video_id": item["id"],
            "title": snippet["title"],
            "channel_title": snippet["channelTitle"],
            "published_at": snippet["publishedAt"],
            "category_id": snippet.get("categoryId"),
            "tags": snippet.get("tags", []),
            "duration": content["duration"],
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "region": snippet.get("regionCode")
        })

    df = pd.DataFrame(rows)
    return df
