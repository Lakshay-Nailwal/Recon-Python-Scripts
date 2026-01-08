import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal
from kafkaPushMessage import create_producer, send_to_kafka

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    with CSV_LOCK:
        append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)

SQL_QUERY = """
SELECT email FROM matrix.user;
"""

def getMatrixUserApproval():
    """Get matrix user approval from the database"""
    try:
        conn = create_db_connection("matrix","PROD8")
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"❌ Error getting matrix user approval: {e}")
        return []

if __name__ == "__main__":
    matrix_user_approval = getMatrixUserApproval()
    
    for user in matrix_user_approval:
        # payload = {"userEmail": user["email"], "clientName": "Mercury"}
        # send_to_kafka(producer, KAFKA_TOPIC, payload)
        # time.sleep(1)