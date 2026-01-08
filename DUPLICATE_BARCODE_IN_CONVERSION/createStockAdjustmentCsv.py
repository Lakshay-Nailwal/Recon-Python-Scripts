import os
import pandas as pd

# Paths
duplicate_folder = "/Users/lakshay.nailwal/Desktop/ReconScripts/DUPLICATE_BARCODE_IN_CONVERSION/CSV_FILES_V3"
backup_folder = "/Users/lakshay.nailwal/Desktop/ReconScripts/DUPLICATE_BARCODE_IN_CONVERSION/BackUp_CSV_FILES_V3"

# -------------------------
# STEP 1: READ DUPLICATE BARCODE FILES
# -------------------------

duplicate_data = []

for file_name in os.listdir(duplicate_folder):
    if file_name.endswith("_duplicate_barcode_analysis.csv"):
        tenant = file_name.split("_duplicate_barcode_analysis")[0]
        file_path = os.path.join(duplicate_folder, file_name)

        df = pd.read_csv(file_path)
        df["tenant"] = tenant
        duplicate_data.append(df)

all_duplicates = pd.concat(duplicate_data, ignore_index=True)

# Keep required columns only
duplicate_cols = ["tenant", "ucode", "batch"]
all_duplicates = all_duplicates[duplicate_cols].drop_duplicates()

# -------------------------
# STEP 2: READ BACKUP FILES WITH qty_per_case
# -------------------------

backup_data = []

for file_name in os.listdir(backup_folder):
    if file_name.endswith("_racker_task_item_backup.csv"):
        tenant = file_name.split("_racker_task_item_backup")[0]
        file_path = os.path.join(backup_folder, file_name)

        df = pd.read_csv(file_path)
        df["tenant"] = tenant

        # Standardize column names to match duplicate file
        df.rename(columns={"batch_number": "batch", "bar_code": "barcode"}, inplace=True)

        backup_data.append(df)

all_backup = pd.concat(backup_data, ignore_index=True)

# -------------------------
# STEP 3: SUM qty_per_case PER TENANT + UCODE + BATCH
# -------------------------

qty_summary = (
    all_backup.groupby(["tenant", "ucode", "batch"])["qty_per_case"]
    .sum()
    .reset_index(name="total_qty_per_case")
)

# -------------------------
# STEP 4: MERGE — KEEP ONLY RECORDS THAT WERE IN DUPLICATE BARCODE FILES
# -------------------------

final_output = duplicate_cols = ["tenant", "ucode", "batch"]
final = all_duplicates.merge(qty_summary, on=["tenant", "ucode", "batch"], how="left")

# -------------------------
# STEP 5: SAVE OUTPUT
# -------------------------

output_file = os.path.join(duplicate_folder, "merged_qty_per_case_summary.csv")
final.to_csv(output_file, index=False)

print(f"✅ Final merged summary created at: {output_file}")