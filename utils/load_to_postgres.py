import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def load_to_postgres(df, region):
    """Upsert records into PostgreSQL trending_videos table."""
    
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )
    
    cursor = conn.cursor()

    rows = df.to_dict('records')
    
    upsert_query = """
        INSERT INTO trending_videos (
            video_id, title, channel_title, published_at, category_id, tags, duration,
            views, likes, comments, region, load_date
        )
        VALUES (
            %(video_id)s, %(title)s, %(channel_title)s, %(published_at)s, %(category_id)s,
            %(tags)s, %(duration)s, %(views)s, %(likes)s, %(comments)s, %(region)s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (video_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            channel_title = EXCLUDED.channel_title,
            published_at = EXCLUDED.published_at,
            category_id = EXCLUDED.category_id,
            tags = EXCLUDED.tags,
            duration = EXCLUDED.duration,
            views = EXCLUDED.views,
            likes = EXCLUDED.likes,
            comments = EXCLUDED.comments,
            region = EXCLUDED.region,
            load_date = CURRENT_TIMESTAMP;
    """

    execute_batch(cursor, upsert_query, rows)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Upserted {len(df)} rows into PostgreSQL for region {region}")
