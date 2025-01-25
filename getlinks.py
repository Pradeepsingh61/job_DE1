# getlinks.py
import pandas as pd
import os
from datetime import datetime

def sanitize_filename(name):
    """
    Allows letters, digits, space, dash, underscore, and dot ('.').
    This ensures that any '.csv' extension remains intact.
    """
    return "".join(c if c.isalnum() or c in (' ', '-', '_', '.') else '_' for c in name).strip().replace(' ', '_')

def extract_links_from_csv(file_path):
    """Extract all http(s) links from the CSV file."""
    try:
        data = pd.read_csv(file_path, dtype=str)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return set()

    # Find columns containing links (any string with 'http://' or 'https://')
    links_data = data.select_dtypes(include=['object']).apply(
        lambda col: col[col.str.contains('https?://', na=False, case=False)]
    )
    # Flatten and remove duplicates
    all_links = links_data.stack().dropna().unique()
    # Strip query strings (e.g., anything after '?')
    cleaned_links = set(link.split('?')[0] for link in all_links)
    return cleaned_links

def append_links_with_timestamp(output_path, new_links):
    """Append new links to the output file with a timestamp header."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_links_df = pd.DataFrame(new_links, columns=["Links"])
    file_exists = os.path.exists(output_path)

    with open(output_path, 'a', encoding='utf-8') as f:
        if not file_exists:
            # If no file exists yet, write header
            new_links_df.to_csv(f, header=True, index=False)
        else:
            # If the file already exists, append data
            if os.path.getsize(output_path) > 0:
                f.write("\n")
            f.write(f"# Last updated: {timestamp}\n")
            new_links_df.to_csv(f, header=False, index=False)
    print(f"Appended {len(new_links)} new links to '{output_path}'.")

def save_only_new_links(output_path, new_links):
    """Save only the new links found in this run."""
    new_links_df = pd.DataFrame(new_links, columns=["Links"])
    new_links_df.to_csv(output_path, index=False)
    print(f"Saved {len(new_links)} NEW links to '{output_path}' (this run only).")

def process_file(source_file):
    """
    Process a single CSV file:
      - Extract links
      - Compare with an existing link file (appended_file)
      - Append new links + create a new-only file (new_only_file)
    """
    # Example:
    #   source_file = "Software_Engineering_full.csv"
    base_name = os.path.splitext(source_file)[0]      # e.g. "Software_Engineering_full"

    # Remove "_full" from the final new-only filename
    # e.g. "Software_Engineering_full" -> "Software_Engineering"
    clean_name = base_name.replace("_full", "")
    appended_file = clean_name + "_links.csv"          # => "Software_Engineering_links.csv"
    new_only_file = clean_name + ".csv"               # => "Software_Engineering.csv"

    if not os.path.exists(source_file):
        print(f"Source file '{source_file}' not found. Skipping.")
        return

    print(f"\nProcessing '{source_file}'...")
    extracted_links = extract_links_from_csv(source_file)
    print(f"Extracted {len(extracted_links)} unique links from '{source_file}'.")

    if not extracted_links:
        print("No links extracted from the source file.")
        return

    # Load existing links (if any)
    existing_links = set()
    if os.path.exists(appended_file):
        try:
            existing_data = pd.read_csv(appended_file, comment='#', header=0)
            if 'Links' in existing_data.columns:
                existing_links = set(existing_data['Links'].dropna().astype(str).unique())
            print(f"Found {len(existing_links)} existing links in '{appended_file}'.")
        except Exception as e:
            print(f"Error reading existing links from '{appended_file}': {e}")
            existing_links = set()

    # Determine which links are truly new
    new_unique_links = extracted_links - existing_links
    if new_unique_links:
        append_links_with_timestamp(appended_file, new_unique_links)
        save_only_new_links(new_only_file, new_unique_links)
    else:
        print("No new unique links to append.")
        # Remove the new-only file if it exists and we have no new links
        if os.path.exists(new_only_file):
            os.remove(new_only_file)
            print(f"Removed '{new_only_file}' as there are no new links this run.")

    print("File processing completed.")

def main():
    file_list_csv = 'files_names.csv'
    
    if not os.path.exists(file_list_csv):
        print(f"Error: '{file_list_csv}' not found.")
        return

    # Read the list of CSV file names from files_names.csv
    try:
        with open(file_list_csv, 'r', encoding='utf-8') as f:
            file_list = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading '{file_list_csv}': {e}")
        return

    # Process each file name
    for csv_file in file_list:
        # Sanitize the filename in case there are invalid chars
        csv_file = sanitize_filename(csv_file)

        # If it doesn't end with .csv, ensure we add it
        # (This step is optional if your file always has .csv)
        if not csv_file.lower().endswith('.csv'):
            csv_file += '.csv'

        process_file(csv_file)

if __name__ == "__main__":
    main()
