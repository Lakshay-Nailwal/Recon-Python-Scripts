import csv
import json
from collections import defaultdict

total_qty = 0
row_count = 0

CSV_FILE = '/Users/lakshay.nailwal/Desktop/ReconScripts/BIN_ALLOCATION_METRIC/CSV_FILES/query_result.csv'

codeCountPerRow = defaultdict(set)

with open(CSV_FILE, 'r') as file:
    csv_reader = csv.DictReader(file)

    for row in csv_reader:
        row_count += 1
        response_payload = row.get('response_payload', '')
        if not response_payload:
            continue

        try:
            payload_data = json.loads(response_payload)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON in row {row_count}: {e}")
            continue

        # Case 1 → response_payload is a list of objects
        if isinstance(payload_data, list):
            for item in payload_data:
                codeCountPerRow[row['bin_allocation_id']].add(item.get('ucode'))

        # Case 2 → response_payload is an object with {"data": [...]}
        elif isinstance(payload_data, dict) and isinstance(payload_data.get("data"), list):
            for item in payload_data["data"]:
                codeCountPerRow[row['bin_allocation_id']].add(item.get('ucode'))

# Print final output
for bin_allocation_id, ucodes in codeCountPerRow.items():
    if(len(ucodes) > 10) :
        # print(f"Bin Allocation ID: {bin_allocation_id}, UCODEs: {len(ucodes)}")
        if(bin_allocation_id == '1126509'):
            print(ucodes)
