import os
from datetime import datetime

def load_to_snowflake(df, region_code):
    """Save dataframe to local CSV file."""
    os.makedirs("data_output", exist_ok=True)
    
    today = datetime.today().strftime("%Y-%m-%d")
    filename = f"data_output/trending_videos_{today}_{region_code}.csv"
    
    # Save to CSV
    df.to_csv(filename, index=False)
    
    print(f"✅ Saved {len(df)} rows to {filename}")
