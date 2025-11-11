import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql
from threading import Lock
from collections import defaultdict
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
CSV_LOCK = Lock()  # Thread-safe CSV writes
BATCH_SIZE = 500

def safe_append_to_csv(filename, rows):
    """Thread-safe CSV append"""
    # with CSV_LOCK:
    append_to_csv(filename, rows, output_dir=CURRENT_DIRECTORY)


def fetchCorrectPdiForReferenceDebitNoteNumber():
    referenceDebitNoteNumberToChildPdiMap = {}
    with open("/Users/lakshay.nailwal/Desktop/ReconScripts/BUG_FIXES_SCRIPTS/PR_STUCK_AT_DISPATCH_READY/CSV_FILES/prStuckAtDispatchReadyWithChildPdi.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            reference_debit_note_number = row["reference_debit_note_number"]
            child_tenant_partner_detail_id = row["child_tenant_partner_detail_id"]
            referenceDebitNoteNumberToChildPdiMap[reference_debit_note_number] = child_tenant_partner_detail_id
    return referenceDebitNoteNumberToChildPdiMap

def process_csv():
    referenceDebitNoteNumberToChildPdiMap = fetchCorrectPdiForReferenceDebitNoteNumber()
    with open("/Users/lakshay.nailwal/Desktop/ReconScripts/BUG_FIXES_SCRIPTS/PR_STUCK_AT_DISPATCH_READY/CSV_FILES/B3_Cases.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            reference_debit_note_number = row["reference_debit_note_number"]
            correctPdi = referenceDebitNoteNumberToChildPdiMap[reference_debit_note_number]
            if correctPdi == "":
                continue
            ogPdi = row["ogPdi"]
            if ogPdi == correctPdi:
                continue
            tenant = row["dest_tenant"]
            purchaseIssueId = row["purchase_issue_id"]

            print("correctPdi: ", correctPdi)

            safe_append_to_csv("updatePdiForStuckPr.csv", {"reference_debit_note_number": reference_debit_note_number, "correctPdi": correctPdi, "ogPdi": ogPdi, "dest_tenant": tenant , "purchaseIssueId": purchaseIssueId , "source_tenant": row["source_tenant"]})

if __name__ == "__main__":
    process_csv()