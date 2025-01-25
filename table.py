# table.py
from airtable_scraper import AirtableScraper
import pandas as pd
import os

def sanitize_filename(name):
    """Sanitize the filename by replacing unwanted characters with underscores."""
    return "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in name).strip().replace(' ', '_')

def main():
    input_csv = 'input_links.csv'
    
    if not os.path.exists(input_csv):
        print(f"Error: '{input_csv}' not found. Please make sure it exists.")
        return

    # Read the CSV with all links
    try:
        links_df = pd.read_csv(input_csv, dtype=str)
    except Exception as e:
        print(f"Error reading '{input_csv}': {e}")
        return

    for idx, row in links_df.iterrows():
        text = row.get('Text')
        url = row.get('Link')

        # Skip any row that doesn't have valid text or link
        if pd.isna(text) or pd.isna(url):
            print(f"Skipping row {idx} due to missing 'Text' or 'Link'.")
            continue

        # Sanitize the text to create a safe CSV filename
        filename = sanitize_filename(text) + "_full.csv"

        # Scrape the Airtable
        try:
            table = AirtableScraper(url=url)
            print(f"Scraping: {text} -> {filename} | Status: {table.status}")
            table.to_csv(filename)
            print(f"Saved data to '{filename}'\n")
        except Exception as e:
            print(f"Failed to scrape {url} for '{text}': {e}")

if __name__ == '__main__':
    main()
