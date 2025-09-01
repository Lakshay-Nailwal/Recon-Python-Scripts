from getDBConnection import create_db_connection
from dotenv import load_dotenv
import os

load_dotenv('config.env')

def getAllArsenal():
    connection = create_db_connection("mercury")
    cursor = connection.cursor()
    cursor.execute("SELECT tenant FROM arsenal WHERE is_setup = 1 order by tenant;")
    result = cursor.fetchall()
    cursor.close()
    connection.close()  
    return [row[0] for row in result]

if __name__ == "__main__":
    allTenant = getAllArsenal()
    print("Total Tenant: ", len(allTenant))