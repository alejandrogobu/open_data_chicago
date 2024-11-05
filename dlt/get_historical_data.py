import dlt
from dlt.destinations import filesystem
from datetime import datetime, timedelta
import requests
import time
import os

# store timestamps without timezone adjustments
os.environ["DATA_WRITER__TIMESTAMP_TIMEZONE"] = ""

# Definir las fechas de inicio y fin
date_start = datetime.fromisoformat('2019-07-15T00:00:00')
date_end = datetime.fromisoformat('2019-07-15T23:59:59')

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

# Bucle que suma un día a las fechas hasta que la fecha de inicio sea igual al día actual
while date_start.date() != datetime.now().date():
     
    # Sumar un día a cada fecha
    date_start += timedelta(days=1)
    date_end += timedelta(days=1)
    
    # Convertir las fechas a formato ISO 8601 con la "T"
    date_start_iso = date_start.isoformat()
    date_end_iso = date_end.isoformat()
    
    # Generar la URL con las fechas en formato correcto
    url = f"https://data.cityofchicago.org/resource/crimes.json?$limit=20000000&$where=updated_on between '{date_start_iso}' and '{date_end_iso}'"
    
    # Imprimir la URL
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
