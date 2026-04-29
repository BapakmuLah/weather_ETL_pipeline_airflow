import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

def load_data(input_path):
    with open(input_path, 'r') as f:
        data = json.load(f)
        
    if not data:
        print("No data to load.")
        return

    df = pd.DataFrame(data)
    
    # GET DATABASE WITH POSTGRES HOOK
    pg_hook = PostgresHook(postgres_conn_id = 'weather_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        insert_main = """
            INSERT INTO airflow_data (
                city_id, city, timezone, longitude, latitude, country, weather, 
                weather_description, weather_icon, temp, feels_like, temp_min, 
                temp_max, pressure, humidity, sea_level, grnd_level, wind_speed, 
                wind_degree, sunrise, sunset, rain, cloudy, base, visibility, 
                datetime_utc, datetime_local, date, year, month, day, hour, 
                day_name, week_of_year, sunrise_utc, sunrise_local, sunset_utc, 
                sunset_local, daylight_duration
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (id) DO NOTHING;
        """
        
        # EXECUTE QUERY (ADD DATA INTO TABLE)
        execute_batch(cursor, insert_main, df.values.tolist())
        conn.commit()
        print("✅ Data berhasil di-load ke PostgreSQL")

    # IF FAILED
    except Exception as e:
        conn.rollback()
        print(f"❌ Error during load: {e}")
        raise e  
    
    # AFTER FINISHED
    finally:
        cursor.close()
        conn.close()