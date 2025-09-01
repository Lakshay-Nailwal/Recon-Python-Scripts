from getDBConnection import create_db_connection
from dotenv import load_dotenv
import os

load_dotenv('config.env')

def getAllWarehouse():
    """Get all warehouse tenants that are set up"""
    try:
        connection = create_db_connection("mercury")
        cursor = connection.cursor()
        cursor.execute("SELECT tenant FROM warehouse WHERE is_setup = 1 ORDER BY tenant;")
        result = cursor.fetchall()
        cursor.close()
        connection.close()
           
        return [row[0] for row in result]
        
    except Exception as e:
        print(f"Error fetching warehouse data: {e}")
        return []

if __name__ == "__main__":
    allTenant = getAllWarehouse()
    print("Total Tenant: ", len(allTenant))
    print("Tenants:", allTenant)