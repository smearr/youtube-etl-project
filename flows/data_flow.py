import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from utils.extract import fetch_trending_videos, save_raw_json, REGIONS
from utils.transform import transform_video_data
from utils.load import load_to_snowflake
from utils.load_to_postgres import load_to_postgres


def run_etl():
    """Run ETL pipeline for all regions."""
    for region in REGIONS:
        print(f"\n{'='*50}")
        print(f"Processing {region}...")
        print(f"{'='*50}")
        
        # Extract
        raw = fetch_trending_videos(region)
        #save_raw_json(raw, region)
        
        # Transform
        df = transform_video_data(raw, region)
        
        # Load (saves to CSV locally)
        load_to_snowflake(df, region)

        #save in postgresql
        load_to_postgres(df, region)
    
    print(f"\n✅ ETL Complete!")

if __name__ == "__main__":
    run_etl()
