import os
import pymysql
from dotenv import load_dotenv
import json

load_dotenv('config.env')

def create_db_connection(db_name):
    try:
        
        DB_NAME = str(db_name).upper()
        config_dict = {}
        config_dict = json.loads(os.getenv(f"{DB_NAME}_DB_CONFIG", "{}"))
        if config_dict == {}:
            config_dict = json.loads(os.getenv(f"MERCURY_DB_CONFIG", "{}"))
            
        host = config_dict['host']
        user = config_dict['user']
        password = config_dict['password']
        port = config_dict['port'] if 'port' in config_dict else 3306
  
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            port=port,
            database=db_name
        )
        return connection

    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise
