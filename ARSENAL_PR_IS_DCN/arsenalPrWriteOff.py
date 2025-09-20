import pandas as pd
import mysql.connector
import requests
import json
import os
import json
from decimal import Decimal
# ---------- CONFIG ----------
OUTPUT_DIR = "invSeqDisOutputs"
CSV_FILE = "/Users/lakshay.nailwal/Desktop/ReconScripts/ARSENAL_PR_IS_DCN/CSV_FILES/deliveryChallanNormalInArsenalPRV2.csv"
UPDATE_FILE = os.path.join(OUTPUT_DIR, "update_queries.sql")
DELETE_FILE = os.path.join(OUTPUT_DIR, "delete_queries.sql")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "enriched_output.csv")
API_RESPONSES_FILE = os.path.join(OUTPUT_DIR, "api_responses.csv")

# DB configs (replace with actual creds)
VAULT_DB_CONFIG = {
    "host": "pe-mercury-vault-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com",
    "user": "dyno_mayank_mehta_pe_drlgt",
    "password": "GPZ6LjUvfMEfPIwA",
    "database": "vault",
    "port": 3306
}

MERCURY_DB_CONFIG={
  "host" : "mercury-prod-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com",
  "user" : "dyno_lakshay_nailwal1_pe_hockc",
  "password" : "tA03EwJf2AUTFNA3",
  "port" : 3306
}

# ---------------------------------

def run_query(conn, query):
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows

def main():
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Step 1: Load CSV
    df = pd.read_csv(CSV_FILE)

    # Step 3: Get unique tenant + debit_note_number
    unique_dn = df[["tenant", "debit_note_number"]].drop_duplicates()

    print(f"Unique DN -> {unique_dn}")

    # DB connections
    vault_conn = mysql.connector.connect(**VAULT_DB_CONFIG)
    mercury_conn = mysql.connector.connect(**MERCURY_DB_CONFIG)

    update_queries = []
    delete_queries = []
    enriched_rows = []
    api_responses = []

    for _, row in unique_dn.iterrows():
        tenant = row["tenant"]
        debit_note_number = row["debit_note_number"]

        # Step 4: Fetch Vault info
        vault_query = f"""
            SELECT dc2.id as vault_dc_id, 
                   dc2.dc_number as vault_dc_number,
                   dc2.amount as vault_dc_amount,
                   dc2.pending_amount as vault_dc_pending_amount,
                   dc2.status as vault_dc_status
            FROM vault.delivery_challan dc2 
            WHERE dc2.dc_number = '{debit_note_number}'
              AND dc2.status = 'OPEN'
              AND dc2.tenant like 'ar%';
        """
        vault_rows = run_query(vault_conn, vault_query)

        if not vault_rows:
            continue  # skip if no open DC in vault

        vault_info = vault_rows[0]

        print(f"Vault info -> {vault_info}")

        # Step 7: API call
        api_url = "https://vault.mercuryonline.co/api/bookkeeper/deliveryChallan/setoff?user=dc71c016-6974-4989-acf7-448591922dcf"
        headers = {
            "Authorization": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJhcHAiOiJuZWJ1bGEiLCJhdWQiOiJtZXJjdXJ5IiwidWlkIjoiY2ZiM2VkZDMtZmUzMS00NDg2LWE2MTctZjlmYjAxOGM2NTQ2IiwiaXNzIjoiUGhhcm1FYXN5LmluIiwibmFtZSI6Ikxha3NoYXkiLCJzdG9yZSI6IjBjY2MxZGNkLTkzM2MtNGQ5OC1hYzk1LTQxN2ExN2U1NjBiYiIsInNjb3BlcyI6WyJ3aC1hZG1pbiIsIndoLXN1cGVyLWFkbWluIl0sImV4cCI6MTc1ODg5MDQyNywidXNlciI6Imxha3NoYXkubmFpbHdhbDFAcGhhcm1lYXN5LmluIiwidGVuYW50IjoidGg0MTEifQ.J18snrbqp6QaAF7IpNP_kQIiwqSDppdqPI2EVhpovYGyu3QmM7fb4s_bngoVfPdUfunPrJl0lNF5oxWxEqFvmw",
            "Content-Type": "application/json"
        }
        payload = {
            "setOffAmt": normalize_value(vault_info["vault_dc_amount"]),
            "refAmt":None,
            "refNum": "Others",
            "refDate":None,
            "remark": "Arsenal PR DCN Write Off",
            "dcId": vault_info["vault_dc_id"],
            "fileList": [],
            "type": "WRITE_OFF",
            "refAmt2":None,
            "refNum2":None,
            "refDate2":None,
            "roundOffAmt": 0
        }
        print(json.dumps(payload, indent=2))
        try:
            response = requests.post(api_url, headers=headers, json=payload)
            api_responses.append({
                "debit_note_number": debit_note_number,
                "status_code": response.status_code,
                "response": response.text,
                "success": response.status_code == 200
            })
            print(f"API call for {debit_note_number}: Status {response.status_code}")
        except Exception as e:
            api_responses.append({
                "debit_note_number": debit_note_number,
                "status_code": None,
                "response": str(e),
                "success": False
            })
            print(f"API call failed for {debit_note_number}: {str(e)}")

    # Save enriched CSV
    enriched_df = pd.DataFrame(api_responses)
    enriched_df.to_csv(OUTPUT_FILE, index=False)

    # # Save API responses
    api_responses_df = pd.DataFrame(api_responses)
    api_responses_df.to_csv(API_RESPONSES_FILE, index=False)

    # # Close DB connections
    vault_conn.close()

    print(f"Processed {len(unique_dn)} debit_note_numbers")

    # Print API call summary
    successful_calls = sum(1 for resp in api_responses if resp["success"])
    failed_calls = len(api_responses) - successful_calls
    print(f"📊 API calls: {successful_calls} successful, {failed_calls} failed")

def normalize_value(val):
    if isinstance(val, Decimal):
        return float(val)  # or str(val) if you want exact representation
    return val

if __name__ == "__main__":
    main()