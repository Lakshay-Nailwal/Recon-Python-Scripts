import mysql.connector
import pandas as pd
import json
from collections import defaultdict
import time
from pdi import pdiToTenantMap
# Load partner_detail_id to tenant mapping
partner_to_tenant = pdiToTenantMap


# Database connection function
def connect_to_database():
    return mysql.connector.connect(
        host="mercury-prod-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com",
        user="dyno_lakshay_nailwal1_pe_hockc",
        password="tA03EwJf2AUTFNA3",
        database="mercury"
    )

# Get tenant list from warehouse table
def get_tenants_list():
    query = "SELECT tenant FROM mercury.warehouse where is_setup = 1 and is_one_roof_enabled = 1"
    connection = connect_to_database()
    cursor = connection.cursor()
    cursor.execute(query)
    tenants = [row[0] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    print(f"Total tenants fetched: {len(tenants)}")
    return tenants

# Run base query per tenant
def run_query_for_tenants(tenants, query):
    all_data = []
    columns = []

    for tenant in tenants:
        print(f"Starting data collection for tenant: {tenant}")
        if "th6" in tenant or tenant in ['th438', 'th997', 'th303']:
            print(f"Skipping excluded tenant: {tenant}")
            continue
        connection = connect_to_database()
        cursor = connection.cursor()
        cursor.execute(f"USE {tenant}")
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        for row in rows:
            partner_id = row[1]  # partner_detail_id
            tenant2 = partner_to_tenant.get(str(partner_id), None)
            all_data.append((tenant, *row, tenant2))

        cursor.close()
        connection.close()
        print(f"Completed data collection for tenant: {tenant}, rows fetched: {len(rows)}")

    return columns + ["MappedTenant"], all_data

# Run join queries using gathered debit_note_number and tenants
from collections import defaultdict

def run_final_join_queries(data):
    grouped = defaultdict(list)
    for row in data:
        tenant1, debit_note_number, partner_detail_id, tenant2 = row
        if tenant2:
            grouped[(tenant1, tenant2)].append(debit_note_number)

    final_results = []
    all_columns = None  # to store columns once

    print(f"Starting join queries for {len(grouped)} tenant pairs...")
    for (tenant1, tenant2), debit_notes in grouped.items():
        unique_debit_notes = list(set(debit_notes))
        print(f"Running join query for tenant pair: ({tenant1}, {tenant2}) with {len(unique_debit_notes)} debit notes")

        connection = connect_to_database()
        cursor = connection.cursor()

        batch_size = 500
        total_batches = (len(unique_debit_notes) + batch_size - 1) // batch_size
        for b in range(0, len(unique_debit_notes), batch_size):
            batch = unique_debit_notes[b:b + batch_size]
            debit_notes_str = ",".join([f"'{dn}'" for dn in batch])

            join_query = f'''
                SELECT 
        '{tenant1}' AS source_tenant, 
        pi.debit_note_number AS source_debit_note_number, 
        pi.partner_detail_id AS internal_vendor_id,
        pi.child_tenant_partner_detail_id AS external_vendor_id,

        grouped_pii.ucode AS source_ucode, 
        grouped_pii.batch AS source_batch, 
        grouped_pii.total_return_quantity AS source_qty,
        grouped_pii.total_amount AS source_DN_amt,

        pi.invoice_date AS source_purchase_issue_invoice_date,
        pi.created_on AS source_created_on,
        pi.updated_on AS source_updated_on,

        '{tenant2}' AS dest_tenant,
        ii.id AS dest_inward_invoice_id,
        ii.invoice_no AS dest_inward_invoice_no,
        ii.status AS dest_inward_invoice_status,
        iii.code AS dest_ii_ucode,
        iii.batch AS dest_ii_batch,
        SUM(iii.total_quantity) AS dest_qty,
        ii.invoice_date AS dest_inward_invoice_date,
        SUM(iii.net_amount) AS dest_inward_invoice_amt,

        pi2.debit_note_number AS dest_DC_no,
        pi2.partner_detail_id AS dest_DC_vendor,
        pi2.invoice_date AS dest_purchase_issue_invoice_date,
        pi2.created_on AS dest_DC_date

    FROM 
        {tenant1}.purchase_issue pi

    JOIN (
        SELECT 
            pi.id as purchase_issue_id,
            pi.debit_note_number,
            pii.ucode,
            pii.batch,
            SUM(pii.return_quantity) AS total_return_quantity,
            SUM(pii.amount) AS total_amount
        FROM 
            {tenant1}.purchase_issue_item pii
        JOIN 
            {tenant1}.purchase_issue pi ON pi.id = pii.purchase_issue_id
        WHERE 
            pi.status IN ('READY_FOR_DELIVERY', 'DELIVERED', 'completed')
            AND pi.debit_note_number IN ({debit_notes_str})
        GROUP BY 
            pi.debit_note_number, pii.ucode, pii.batch
    ) grouped_pii ON pi.id = grouped_pii.purchase_issue_id

    LEFT JOIN 
        {tenant2}.inward_invoice ii ON ii.invoice_no = pi.debit_note_number

    LEFT JOIN 
        {tenant2}.inward_invoice_item iii 
        ON ii.id = iii.invoice_id
        AND grouped_pii.ucode = iii.code
        AND grouped_pii.batch = iii.batch

    LEFT JOIN
        {tenant2}.purchase_issue pi2 ON pi2.source_invoice_id = ii.id

    WHERE 
        pi.status IN ('READY_FOR_DELIVERY', 'DELIVERED', 'completed')
        AND pi.debit_note_number IN ({debit_notes_str})

    GROUP BY 
        pi.debit_note_number, grouped_pii.ucode, grouped_pii.batch;
            '''

            try:
                cursor.execute(join_query)
                rows = cursor.fetchall()
                if rows:
                    if all_columns is None:
                        all_columns = [desc[0] for desc in cursor.description]
                    final_results.extend(rows)
                print(f"Batch {(b // batch_size) + 1}/{total_batches} returned {len(rows)} rows for ({tenant1}, {tenant2})")
            except Exception as e:
                print(f"Error in tenant pair ({tenant1}, {tenant2}) batch {(b // batch_size) + 1}: {e}")

        cursor.close()
        connection.close()

    print("Join queries completed.")
    return all_columns, final_results


# Main pipeline
def main():
    print("Starting main process...")
    tenants = ['th408']
    base_query = '''
        SELECT distinct debit_note_number, partner_detail_id FROM purchase_issue 
        WHERE status NOT IN ('cancelled') 
        AND debit_note_number IS NOT NULL AND debit_note_number NOT IN ('')
        AND invoice_date > '2025-05-27 00:00:00'
        AND created_on > '2025-05-27 00:00:00'
    '''

    columns, base_data = run_query_for_tenants(tenants, base_query)
    df = pd.DataFrame(base_data, columns=["Tenant"] + columns)
    df.to_csv("debit_note_with_tenant_mapping.csv", index=False)
    print(f"Exported base data to debit_note_with_tenant_mapping.csv with {len(df)} rows")

    columns, join_results = run_final_join_queries(base_data)
    final_df = pd.DataFrame(join_results, columns=columns)
    fileName = str(int(time.time())) + "_str_data_linkage.csv"
    final_df.to_csv(fileName, index=False)

    print(f"Exported join query results to {fileName} with {len(final_df)} rows")

    print("All processing completed successfully.")

if __name__ == "__main__":
    main()
