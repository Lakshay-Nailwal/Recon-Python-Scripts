import json

INPUT_JSON = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/invoice_items_v8.jsonl"
OUTPUT_SQL = "/Users/lakshay.nailwal/Desktop/ReconScripts/SR_EMPTY_INWARD_INVOICE_ITEMS/CSV_FILES/chunked_invoice_item_insert_v8.sql"

CHUNK_SIZE = 2000

SQL_COLUMNS = [
    "invoice_id", "code", "name", "manufacturer_name", "qty_per_pack", "pack_quantity",
    "refrigerated", "batch", "expiry_date", "mrp", "purchase_rate", "quantity",
    "scheme_quantity", "total_quantity", "discount", "discount_amount",
    "scheme_discount", "scheme_discount_amount", "purchase_rate_after_scheme",
    "purchase_rate_after_discount", "effective_purchase_rate", "abetted_mrp",
    "margin", "tax", "total_tax", "total_amount", "net_amount", "hsn", "sgst",
    "cgst", "igst", "sgst_amount", "cgst_amount", "igst_amount", "supplier_item_code",
    "supplier_item_name", "barcode", "bin", "item_order", "banned", "issue_quantity",
    "pack_type", "po_id", "parent_item_id", "ptr", "previous_margin", "return_reason"
]

# ---------- TYPE CONVERTERS (STRICT – match MySQL schema) ----------

def convert_int(v):
    try:
        return int(v)
    except:
        return None

def convert_float(v):
    try:
        return float(v)
    except:
        return None

def convert_bool(v):
    if v in [True, 1, "1", "true", "TRUE", "True"]:
        return 1
    if v in [False, 0, "0", "false", "FALSE", "False"]:
        return 0
    return None

def sql_val(v):
    """SQL-safe literal generation with strict type handling."""
    if v is None:
        return "NULL"

    if isinstance(v, (int, float)):
        return str(v)

    if isinstance(v, bool):
        return "1" if v else "0"

    s = str(v).replace("'", "''")
    return f"'{s}'"


# ---------- MAP JSON TO SQL ROW ----------

def map_json_to_tuple(parent):
    item = parent.get("item", {})

    mapping = {
        # BIGINT
        "invoice_id": convert_int(parent.get("invoice_id")),

        # VARCHAR / CHAR
        "code": item.get("code"),
        "name": item.get("name"),
        "manufacturer_name": item.get("manufacturerName"),

        # INT
        "qty_per_pack": convert_int(item.get("quantityPerPack")),

        # VARCHAR
        "pack_quantity": item.get("packQuantity"),

        # TINYINT(1)
        "refrigerated": convert_bool(item.get("refrigerated")),

        # VARCHAR
        "batch": item.get("batch"),

        # DATE
        "expiry_date": item.get("expiryDate"),

        # DECIMAL
        "mrp": convert_float(item.get("mrp")),
        "purchase_rate": convert_float(item.get("purchaseRate")),

        # INT
        "quantity": convert_int(item.get("quantity")),
        "scheme_quantity": convert_int(item.get("schemeQuantity")),
        "total_quantity": convert_int(item.get("totalQuantity")),

        # DECIMAL
        "discount": convert_float(item.get("discount")),
        "discount_amount": convert_float(item.get("itemDiscountAmount")),
        "scheme_discount": convert_float(item.get("schemeDiscountPercentage")),
        "scheme_discount_amount": convert_float(item.get("schemeDiscount")),
        "purchase_rate_after_scheme": convert_float(item.get("purchaseRateAfterScheme")),
        "purchase_rate_after_discount": convert_float(item.get("purchaseRateAfterDiscount")),
        "effective_purchase_rate": convert_float(item.get("effectivePurchaseRate")),
        "abetted_mrp": convert_float(item.get("abettedMrp")),
        "margin": convert_float(item.get("margin")),
        "tax": convert_float(item.get("tax")),
        "total_tax": convert_float(item.get("totalTax")),
        "total_amount": convert_float(item.get("totalAmount")),
        "net_amount": convert_float(item.get("netAmount")),

        # VARCHAR
        "hsn": item.get("hsn"),

        # DECIMAL
        "sgst": convert_float(item.get("sgst")),
        "cgst": convert_float(item.get("cgst")),
        "igst": convert_float(item.get("igst")),
        "sgst_amount": convert_float(item.get("sgstAmount")),
        "cgst_amount": convert_float(item.get("cgstAmount")),
        "igst_amount": convert_float(item.get("igstAmount")),

        # VARCHAR
        "supplier_item_code": item.get("supplierItemCode"),
        "supplier_item_name": item.get("supplierItemName"),
        "barcode": item.get("barcode"),
        "bin": item.get("bin"),

        # INT
        "item_order": convert_int(item.get("itemOrder")),

        # TINYINT
        "banned": convert_bool(item.get("banned")),

        # INT
        "issue_quantity": convert_int(item.get("issueQuantity")),

        # VARCHAR
        "pack_type": item.get("packType"),

        # BIGINT
        "po_id": convert_int(item.get("poId")),
        "parent_item_id": convert_int(item.get("parentItemId")),

        # DECIMAL
        "ptr": convert_float(item.get("ptr")),
        "previous_margin": convert_float(item.get("previousMargin")),

        # VARCHAR
        "return_reason": item.get("returnReason"),
    }

    vals = [sql_val(mapping[col]) for col in SQL_COLUMNS]
    return "(" + ", ".join(vals) + ")"


# ---------- PROCESS JSONL → CHUNKED SQL ----------

def process():
    tenant_buffers = {}
    rows = []

    with open(INPUT_JSON, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    print(f"Loaded {len(rows)} JSON rows")

    # group by tenant
    for row in rows:
        tenant = row.get("tenant")
        if not tenant:
            print("❌ Missing tenant key in row, skipping.")
            continue
        tenant_buffers.setdefault(tenant, []).append(map_json_to_tuple(row))

    with open(OUTPUT_SQL, "w") as sqlf:
        cols = ", ".join(SQL_COLUMNS)

        for tenant, tuples in tenant_buffers.items():
            print(f"Tenant={tenant} | Rows={len(tuples)}")

            for i in range(0, len(tuples), CHUNK_SIZE):
                chunk = tuples[i:i + CHUNK_SIZE]

                sqlf.write(f"INSERT INTO `{tenant}`.inward_invoice_item ({cols}) VALUES\n")
                sqlf.write(",\n".join(chunk))
                sqlf.write(";\n\n")

    print("✅ DONE — output SQL saved at:", OUTPUT_SQL)


if __name__ == "__main__":
    process()