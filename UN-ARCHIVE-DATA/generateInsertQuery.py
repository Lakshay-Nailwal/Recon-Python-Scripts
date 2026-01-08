import json

CHUNK_SIZE = 2000
TABLE_NAME = "aggregated_racker_tasks"

INPUT_JSON = "/Users/lakshay.nailwal/Desktop/ReconScripts/UN-ARCHIVE-DATA/data.json"
OUTPUT_SQL = "/Users/lakshay.nailwal/Desktop/ReconScripts/UN-ARCHIVE-DATA/bulk_insert_v2.sql"

with open(INPUT_JSON, "r") as f:
    raw = json.load(f)

rows = raw["data"]

# Final column list as per aggregated_racker_task_item table
columns =[
  "id",
  "ucode",
  "bin",
  "batch",
  "status",
  "racker_task_id",
  "created_by",
  "updated_by_name",
  "updated_by",
  "created_on",
  "updated_on",
  "is_archived",
  "dp_updated_at",
  "case_id",
  "case_type",
  "qty_per_case"
]


def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)

queries = []

for i in range(0, len(rows), CHUNK_SIZE):
    chunk = rows[i:i + CHUNK_SIZE]
    values_sql = []

    for r in chunk:

        # Force un-archive
        r["is_archived"] = 0

        values = [sql_value(r.get(col)) for col in columns]
        values_sql.append(f"({', '.join(values)})")

    newline = '\n'
    values_str = f",{newline}".join(values_sql)
    query = (
        f"INSERT INTO {TABLE_NAME} ({', '.join(columns)}){newline}"
        f"VALUES{newline}"
        f"{values_str};"
    )

    queries.append(query)

with open(OUTPUT_SQL, "w") as f:
    f.write("\n\n".join(queries))

print(f"Generated {len(queries)} INSERT queries for table `{TABLE_NAME}`")
