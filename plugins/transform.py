import json
from datetime import datetime, timedelta, timezone

def transform_data(input_path, output_path):
    with open(input_path, 'r') as f:
        data = json.load(f)

    # FOR EACH DATA IN JSON FILE
    for row in data:
        # UTC DATETIME
        datetime_utc = datetime.fromtimestamp(row['datetime'], tz=timezone.utc)
        row['datetime_utc'] = datetime_utc.isoformat() # Convert to string agar JSON serializable

        # Local Time
        tz_offset = timedelta(seconds=row['timezone'])
        datetime_local = datetime_utc + tz_offset
        row['datetime_local'] = datetime_local.isoformat()

        row['date'] = str(datetime_local.date())
        row['year'] = datetime_local.year
        row['month'] = datetime_local.month
        row['day'] = datetime_local.day
        row['hour'] = datetime_local.hour
        row['day_name'] = datetime_local.strftime('%A')
        row['week_of_year'] = datetime_local.isocalendar()[1]

        # Sunrise & Sunset
        sunrise_utc = datetime.fromtimestamp(row['sunrise'], tz=timezone.utc)
        row['sunrise_utc'] = sunrise_utc.isoformat()
        sunrise_local = sunrise_utc + tz_offset
        row['sunrise_local'] = sunrise_local.isoformat()

        sunset_utc = datetime.fromtimestamp(row['sunset'], tz=timezone.utc)
        row['sunset_utc'] = sunset_utc.isoformat()
        sunset_local = sunset_utc + tz_offset
        row['sunset_local'] = sunset_local.isoformat()

        # Daylight
        row['daylight_duration'] = ((sunset_local - sunrise_local).total_seconds() / 3600)

        row.pop('datetime', None)

    # SAVE TRANSFORMED FILE INTO JSON FILE
    with open(output_path, 'w') as f:
        json.dump(data, f)
        
    print(f"Transformation complete. Data saved to {output_path}")