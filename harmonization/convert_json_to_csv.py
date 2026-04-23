import json
import csv
import os

def convert_json_to_csv(json_file_path, csv_file_path):
    print(f"Reading JSON file: {json_file_path}")
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not data:
        print("JSON file is empty.")
        return

    # The JSON structure is { "DrugName": { ... details ... }, "DrugName2": { ... } }
    # We want to convert the values (details) into CSV rows.
    
    # Extract the list of records
    records = list(data.values())
    
    if not records:
        print("No records found in JSON.")
        return

    # Determine all possible headers
    headers = set()
    for record in records:
        headers.update(record.keys())
    
    # Sort headers for consistent output
    # Prioritize 'query_name' and 'harmonized_drug_name' if they exist
    sorted_headers = sorted(list(headers))
    priority_cols = ['query_name', 'harmonized_drug_name', 'harmonized_generic_name', 'harmonized_brand_name']
    for col in reversed(priority_cols):
        if col in sorted_headers:
            sorted_headers.insert(0, sorted_headers.pop(sorted_headers.index(col)))

    print(f"Writing parsed data to CSV: {csv_file_path}")
    print(f"Columns: {sorted_headers}")

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sorted_headers)
        writer.writeheader()

        for record in records:
            row = {}
            for key in sorted_headers:
                val = record.get(key)
                
                # Handle lists and nested structures
                if isinstance(val, list):
                    # Check if it's a list of strings or list of objects
                    if val and isinstance(val[0], (dict, list)):
                        # For complex nested structures (like 'applications'), dump as JSON string
                        row[key] = json.dumps(val)
                    else:
                        # For lists of strings/numbers, join them
                        # Filter out None values just in case
                        clean_val = [str(v) for v in val if v is not None]
                        row[key] = " | ".join(clean_val)
                elif isinstance(val, dict):
                    # For dict values, dump as JSON string
                    row[key] = json.dumps(val)
                else:
                    # Simple values
                    row[key] = val
            writer.writerow(row)

    print("Conversion complete.")

if __name__ == "__main__":
    json_path = r"d:\CT_FDA\data_pipeline\harmonization\test_harmonization_output_20260125_203412.json"
    csv_path = r"d:\CT_FDA\data_pipeline\harmonization\test_harmonization_output_20260125_203412.csv"
    convert_json_to_csv(json_path, csv_path)
