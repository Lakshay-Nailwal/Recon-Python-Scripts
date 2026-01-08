import requests
import time
import json

INPUT_FILE = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/srEmptyInwardInvoiceItems_v8.csv"
OUTPUT_JSON = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/invoice_items_v8.jsonl"

API_URL = "http://localhost:8080/return/test"
DELAY_BETWEEN_CALLS = 0.1  # seconds


def load_unique_rows():
    unique_set = set()
    unique_rows = []

    import csv
    with open(INPUT_FILE, "r", newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row.get("invoice_status", "")
            key = (row["invoice_id"], row["all_return_order_id"], row["tenant"])
            if key not in unique_set:
                unique_set.add(key)
                unique_rows.append(row)
    return unique_rows


def fetch_api_items(order_id, tenant):
    try:
        resp = requests.get(API_URL, params={"id": order_id}, headers={"x-tenant": tenant}, timeout=180)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as e:
        print(f"❌ API error for tenant={tenant}, order={order_id}: {e}")
        return []


def process():
    rows = load_unique_rows()
    print(f"Total unique rows to process: {len(rows)}")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        for count, row in enumerate(rows, start=1):
            tenant = row["tenant"]
            invoice_id = row["invoice_id"]
            order_id = row["all_return_order_id"]

            print(f"➡️ {count}/{len(rows)} Processing | tenant={tenant} | order={order_id} | invoice={invoice_id}")

            items = fetch_api_items(order_id, tenant)
            for item in items:
                # Add tenant & invoice info
                record = {
                    "tenant": tenant,
                    "invoice_id": invoice_id,
                    "all_return_order_id": order_id,
                    "item": item
                }
                # Write as JSON line
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            time.sleep(DELAY_BETWEEN_CALLS)

    print(f"✅ JSONL saved successfully: {OUTPUT_JSON}")


if __name__ == "__main__":
    process()
