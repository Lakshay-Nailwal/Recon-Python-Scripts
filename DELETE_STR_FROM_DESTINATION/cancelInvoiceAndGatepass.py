import sys
import os
import csv
import pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/ReconScripts/DELETE_STR_FROM_DESTINATION/CSV_FILES/input.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/ReconScripts/DELETE_STR_FROM_DESTINATION/CSV_FILES"

def getInvoiceDetails(db_name, source_debit_note_number):
    connection = create_db_connection(db_name)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM inward_invoice WHERE invoice_no = %s and status = 'StockHidden'",
                (source_debit_note_number,)
            )
            return cursor.fetchall()
    finally:
        connection.close()

def getGatepassDetails(db_name, source_debit_note_number, gatepass_id):
    connection = create_db_connection(db_name)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM gatepass_invoice WHERE gatepass_id = %s and no = %s",
                (gatepass_id, source_debit_note_number)
            )
            return cursor.fetchall()
    finally:
        connection.close()

# Worker function to process one row
def process_row(row, alreadyProcessedDN):
    # CSV has source_tenant column which contains the destination tenant values
    db_name = row.get("dest_tenant") or row.get("source_tenant")
    invoice_no = row["invoice_no"]
    # source_tenant for output (use empty if dest_tenant exists in CSV, otherwise use source_tenant value)
    source_tenant = row.get("source_tenant", "") if "dest_tenant" in row else ""

    if invoice_no in alreadyProcessedDN:
        return None  # skip if already processed

    alreadyProcessedDN.add(invoice_no)  # use set for fast lookup
    print("tenant : ", db_name, "invoice_no : ", invoice_no)

    cancelledInvoices = []
    cancelledGatepasses = []
    failedDNs = []

    invoiceDetails = getInvoiceDetails(db_name, invoice_no)
    if len(invoiceDetails) > 0:
        for invoiceDetail in invoiceDetails:
            if invoiceDetail["status"] == "StockHidden":
                cancelInvoiceQuery = (
                    f"UPDATE {db_name}.inward_invoice SET status = 'CANCELLED' , updated_on = NOW() "
                    f"WHERE id = {invoiceDetail['id']} and invoice_no = '{invoice_no}' "
                    f"and status = 'StockHidden';"
                )
                cancelledInvoices.append([invoice_no, db_name, "", cancelInvoiceQuery])

                gatepassDetails = getGatepassDetails(db_name, invoice_no, invoiceDetail["gatepass_id"])
                if len(gatepassDetails) > 0:
                    for gatepassDetail in gatepassDetails:
                        cancelGatepassQuery = (
                            f"UPDATE {db_name}.gatepass_invoice SET status = 'CANCELLED' , updated_on = NOW() "
                            f"WHERE id = {gatepassDetail['id']} and no = '{invoice_no}' "
                            f"and status = '{gatepassDetail['status']}';"
                        )
                        cancelledGatepasses.append([invoice_no, db_name, "", cancelGatepassQuery])
    else:
        failedDNs.append([invoice_no, db_name, source_tenant])

    return (cancelledInvoices, cancelledGatepasses, failedDNs)


def process_csv():
    cancelledInvoiceNumbers = []
    cancelledGatepassNumbers = []
    failedDebitNoteNumbers = []
    alreadyProcessedDN = set()

    with open(INPUT_CSV, newline='') as infile:
        reader = list(csv.DictReader(infile))  # load rows first

    # Use ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_row, row, alreadyProcessedDN) for row in reader]

        for future in as_completed(futures):
            result = future.result()
            if result:
                invoices, gatepasses, failed = result
                cancelledInvoiceNumbers.extend(invoices)
                cancelledGatepassNumbers.extend(gatepasses)
                failedDebitNoteNumbers.extend(failed)

    # Save CSVs (single-threaded)
    save_to_csv("cancelledInvoiceNumbers_STR.csv",
                cancelledInvoiceNumbers,
                ["invoice_no", "dest_tenant", "source_tenant", "cancelInvoiceQuery"],
                OUTPUT_DIR)

    save_to_csv("cancelledGatepassNumbers_STR.csv",
                cancelledGatepassNumbers,
                ["invoice_no", "dest_tenant", "source_tenant", "cancelGatepassQuery"],
                OUTPUT_DIR)

    save_to_csv("failedDebitNoteNumbers_STR.csv",
                failedDebitNoteNumbers,
                ["invoice_no", "dest_tenant", "source_tenant"],
                OUTPUT_DIR)

    print("total already processed debit note numbers : ", len(alreadyProcessedDN))


if __name__ == "__main__":
    process_csv()
