import requests
import csv

def get_wayback_urls_to_csv(target_url, start_date="19940101", end_date="20240101", output_file="urls.csv", batch_size=100000, start_url=None):
    """
    Fetches URLs from the Wayback Machine for a given target URL and saves them to a CSV file in batches.

    Args:
        target_url (str): The website URL to fetch archived URLs for.
        start_date (str): Start date in the format YYYYMMDD.
        end_date (str): End date in the format YYYYMMDD.
        output_file (str): The CSV file to save the URLs to.
        batch_size (int): The number of URLs to fetch in each batch.
        start_url (str): A specific URL to start fetching from, if resuming.

    Returns:
        int: The total number of URLs saved to the file.
    """
    cdx_api = "http://web.archive.org/cdx/search/cdx"
    params = {
        'url': target_url,
        'from': start_date,
        'to': end_date,
        'output': 'json',
        'fl': 'original',
        'limit': str(batch_size),
    }
    
    if start_url:
        params['last'] = start_url  # Start fetching from this specific URL if provided
    
    total_urls = 0
    session = requests.Session()

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["URL"])  # Write header
        
        while True:
            response = session.get(cdx_api, params=params)
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                break

            data = response.json()
            if len(data) <= 1:
                break  # No more data to fetch

            urls = [entry[0] for entry in data[1:]]
            writer.writerows([[url] for url in urls])  # Write URLs to CSV

            total_urls += len(urls)
            print(f"Fetched and saved {total_urls} URLs so far...")

            # Update parameters to fetch the next batch
            params['last'] = urls[-1]

    print(f"Finished fetching. Total URLs saved: {total_urls}")
    return total_urls

# Example usage
url_count = get_wayback_urls_to_csv(
    target_url="*.8op.com/*",  # Target site URL pattern
    start_date="20200101",   # Start date (YYYYMMDD)
    end_date="20231231",     # End date (YYYYMMDD)
    output_file="urls.csv",
    start_url=None           # If resuming, provide the last URL fetched
)

print(f"Saved {url_count} URLs to the CSV file.")
