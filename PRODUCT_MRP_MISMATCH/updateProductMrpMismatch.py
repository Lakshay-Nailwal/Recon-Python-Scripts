import sys
import os
import csv
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection  # not used, but kept if needed later
from csv_utils import append_to_csv               # replaced with direct file writes for SQL
from getAllWarehouse import getAllWarehouse       # not used here, safe to remove if unnecessary
from getAllArsenal import getAllArsenal           # not used here, safe to remove if unnecessary


CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
BATCH_SIZE = 2000

# Input CSVs
inventoryMismatchCsv = "/Users/lakshay.nailwal/Desktop/ReconScripts/PRODUCT_MRP_MISMATCH/CSV_FILES/product_inventory_mismatch_mrp_v3.csv"
productLotMismatchCsv = "/Users/lakshay.nailwal/Desktop/ReconScripts/PRODUCT_MRP_MISMATCH/CSV_FILES/product_lot_mismatch_mrp_v3.csv"


def generateInventoryUpdateQuery(tenant, ids):
    return f"""
        UPDATE {tenant}.product_inventory pi 
        JOIN {tenant}.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN ({",".join(ids)});
    """.strip()


def generateProductLotUpdateQuery(tenant, ids):
    return (f"""UPDATE {tenant}.product_lot pl 
        JOIN {tenant}.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN ({",".join(ids)});
    """).strip()


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def process_csv(filename, entity):
    with open(filename, "r") as file:
        reader = csv.DictReader(file)
        chunkIdsAtTenant = defaultdict(list)

        # Collect ids per tenant
        for row in reader:
            tenant = row["tenant"]
            id = row["id"].strip()
            if id.isdigit():
                chunkIdsAtTenant[tenant].append(id)

        # Generate update queries
        for tenant, ids in chunkIdsAtTenant.items():
            for chunkIds in chunk_list(ids, BATCH_SIZE):
                if entity == "inventory":
                    updateQuery = generateInventoryUpdateQuery(tenant, chunkIds)
                    output_file = "update_product_inventory_mrp_mismatch_v4.sql"
                else:
                    updateQuery = generateProductLotUpdateQuery(tenant, chunkIds)
                    output_file = "update_product_lot_mrp_mismatch_v4.sql"

                # Append queries to output file
                with open(os.path.join(CURRENT_DIRECTORY, output_file), "a") as f:
                    f.write(updateQuery + "\n")


if __name__ == "__main__":
    # Process both mismatch CSVs
    process_csv(inventoryMismatchCsv, "inventory")
    process_csv(productLotMismatchCsv, "lot")
