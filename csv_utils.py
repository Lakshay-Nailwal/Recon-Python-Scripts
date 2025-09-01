import csv
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config.env')

# Get output directory from environment variable, default = current directory
OUTPUT_DIRECTORY = os.path.join(os.getcwd(), "CSV_FILES")


def save_to_csv(filename, data, headers=None, output_dir=None):
    """
    Save data to CSV file. Supports both list of dicts and list of lists/tuples.
    Auto-picks headers if not provided.
    """
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY
    
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)

    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as csvfile:
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Auto-pick headers if not given
                if headers is None:
                    headers = list(data[0].keys())

                sanitized_data = [
                    {k: v for k, v in row.items() if k.strip() != ''}
                    for row in data
                ]
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(sanitized_data)

            else:
                # Handle list of lists/tuples
                if headers is None and data:
                    headers = [f"col{i+1}" for i in range(len(data[0]))]

                writer = csv.writer(csvfile)
                if headers:
                    writer.writerow(headers)
                writer.writerows(data)
        
        print(f"✅ CSV file saved: {full_path}")
        print(f"Total rows written: {len(data)}")
        return full_path

    except Exception as e:
        print(f"❌ Error saving CSV file: {e}")
        raise


def append_to_csv(filename, data, headers=None, output_dir=None, needLogs=True):
    """
    Append row(s) to CSV file.
    Supports both list of dicts and list of lists/tuples.
    Auto-picks headers if not provided.
    """
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY

    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    file_exists = os.path.isfile(full_path)

    # Normalize data into list form
    if isinstance(data, dict):
        data = [data]
    elif isinstance(data, (list, tuple)) and data and isinstance(data[0], (str, int, float)):
        # single row like ["a", "b", "c"]
        data = [list(data)]

    try:
        with open(full_path, 'a', newline='', encoding='utf-8') as csvfile:
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Auto-pick headers from dict keys
                if headers is None:
                    headers = list(data[0].keys())

                sanitized_data = [
                    {k: v for k, v in row.items() if k.strip() != ''}
                    for row in data
                ]

                writer = csv.DictWriter(csvfile, fieldnames=headers)

                # If file does not exist, write headers first
                if not file_exists:
                    writer.writeheader()

                writer.writerows(sanitized_data)

            else:
                # Handle list of lists/tuples
                if headers is None and data:
                    headers = [f"col{i+1}" for i in range(len(data[0]))]

                writer = csv.writer(csvfile)

                # If file does not exist, write headers first
                if not file_exists and headers:
                    writer.writerow(headers)

                if data and isinstance(data[0], (list, tuple)):
                    writer.writerows(data)   # multiple rows
                else:
                    writer.writerow(data)    # single row

        if needLogs:
            print(f"✅ Data appended to: {full_path}")
            rows_added = len(data) if isinstance(data, list) else 1
            print(f"Rows appended: {rows_added}")

        return full_path

    except Exception as e:
        print(f"❌ Error appending to CSV file: {e}")
        raise


# Example usage
if __name__ == "__main__":
    # Case 1: list of dicts (headers auto from keys)
    data_dicts = [
        {"Name": "John", "Age": 25, "City": "NY"},
        {"Name": "Jane", "Age": 30, "City": "London"}
    ]
    save_to_csv("dict_data.csv", data_dicts)

    # Case 2: list of lists (headers auto-generated col1,col2,...)
    data_lists = [
        ["John", 25, "NY"],
        ["Jane", 30, "London"]
    ]
    save_to_csv("list_data.csv", data_lists)

    # Case 3: explicitly pass headers (overrides auto)
    save_to_csv("custom_headers.csv", data_lists, headers=["Name", "Age", "City"])
