import dlt
from dlt.destinations import filesystem
from datetime import datetime, timedelta
import requests
import time
import os

# Store timestamps without timezone adjustments
os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = ""

# Define start and end dates
date_start = datetime.fromisoformat('2024-10-25T00:00:00')
date_end = date_start + timedelta(hours=23, minutes=59, seconds=59)

# Define a DLT resource to fetch crime data from the Chicago dataset API
@dlt.resource(
    table_name="crimes",               # Name of the table to store the data
    write_disposition="append",
    file_format="parquet"              
)
def get_crimes(url):
    # Make the API request and raise an exception if the request fails
    response = requests.get(url)
    response.raise_for_status()        # Ensure the response status is OK
    yield response.json()

# Loop that adds a day to the dates until the start date equals the current day
while date_start.date() != datetime.now().date():
     
    # Add a day to each date
    date_start += timedelta(days=1)
    date_end += timedelta(days=1)
    
    # Convert dates to ISO 8601 format with "T"
    date_start_iso = date_start.isoformat()
    date_end_iso = date_end.isoformat()
    
    # Generate the URL with the dates in correct format
    url = f"https://data.cityofchicago.org/resource/crimes.json?$limit=20000000&$where=updated_on between '{date_start_iso}' and '{date_end_iso}'"
    
    # Print the URL
    print(url)

    pipeline = dlt.pipeline(
    pipeline_name="chicago_crimes",
    destination=filesystem(
        layout="{table_name}/year={year}/month={month}/day={day}/{load_id}.{file_id}.{ext}",
        extra_placeholders={
            "year": f"{date_start.year}",
            "month": f"{date_start.month}",
            "day": f"{date_start.day}",
                    }
                ),
    dataset_name= "chicago_crimes"
            )
    start_time = time.time()
    load_info = pipeline.run(get_crimes(url))
    end_time = time.time()
    elapsed_time = end_time - start_time
    # Print the load information and the last trace's normalization details
    print(f"Time taken to execute pipeline: {elapsed_time} seconds")
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)