import os
import pandas as pd

# ==============================
# CONFIG
# ==============================
CURRENT_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "CSV_FILES"
)

CSV_FILE = os.path.join(CURRENT_DIRECTORY, "purchase_issues_v2.csv")
QUERY_RESULT_FILE = os.path.join(CURRENT_DIRECTORY, "query_result.json")
OUTPUT_FILE = os.path.join(CURRENT_DIRECTORY, "purchase_issues_v2_enriched.csv")


# ==============================
# MAIN
# ==============================
def main():
    print("🚀 Starting CSV enrichment...")
    
    # Load CSV file
    print(f"📖 Loading CSV from {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    print(f"✅ Loaded {len(df)} rows from CSV")
    
    # Load query result file (it's actually a CSV file despite the .json extension)
    print(f"📖 Loading query result from {QUERY_RESULT_FILE}...")
    query_df = pd.read_csv(QUERY_RESULT_FILE)
    print(f"✅ Loaded {len(query_df)} partner records from query result")
    
    # Create a mapping dictionary from partner_detail_id (id column) to the data
    partner_mapping = {}
    for _, row in query_df.iterrows():
        partner_id = row.get("id")
        if pd.notna(partner_id):
            partner_mapping[int(partner_id)] = {
                "partner_name": row.get("name", ""),
                "retailer_type": row.get("GROUP_CONCAT( distinct pft.type )", "")
            }
    
    print(f"✅ Created mapping for {len(partner_mapping)} partner records")
    
    # Add new columns to the dataframe
    print("🔄 Enriching CSV with query result data...")
    
    def get_partner_data(partner_id, field):
        """Helper function to safely get partner data"""
        if pd.isna(partner_id):
            return ""
        try:
            partner_id_int = int(partner_id)
            return partner_mapping.get(partner_id_int, {}).get(field, "")
        except (ValueError, TypeError):
            return ""
    
    df["partner_name"] = df["partner_detail_id"].apply(
        lambda x: get_partner_data(x, "partner_name")
    )
    df["retailer_type"] = df["partner_detail_id"].apply(
        lambda x: get_partner_data(x, "retailer_type")
    )
    
    # Count how many rows were successfully enriched
    enriched_count = df[df["partner_name"] != ""].shape[0]
    print(f"✅ Enriched {enriched_count} out of {len(df)} rows")
    
    # Save the enriched CSV
    print(f"💾 Saving enriched CSV to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Successfully saved enriched CSV with {len(df)} rows")
    print("🎯 Enrichment complete!")


if __name__ == "__main__":
    main()

