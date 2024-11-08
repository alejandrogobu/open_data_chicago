
from datetime import datetime, timedelta

date_start = datetime.fromisoformat('2024-10-25T00:00:00')
date_end = date_start + timedelta(hours=23, minutes=59, seconds=59)

print("Start:", date_start)
print("End:", date_end)