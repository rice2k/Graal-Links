import requests
import csv

def get_wayback_urls_to_csv(target_url, start_date="19960101", end_date="20240101", output_file="urls.csv", batch_size=100000, start_url=None):
    """
    Fetches URLs from the Wayback Machine for a given target URL and saves them to a CSV file in batches.

    Args:
        target_url (str): The website URL pattern to fetch archived URLs for (e.g., '*graalonline.com/*').
        start_date (str): Start date for the search, formatted as YYYYMMDD.
        end_date (str): End date for the search, formatted as YYYYMMDD.
        output_file (str): The name of the CSV file where the URLs will be saved.
        batch_size (int): The number of URLs to fetch in each batch to avoid memory overload.
        start_url (str): The specific URL to start fetching from, useful if resuming from a previous run.

    Returns:
        int: The total number of URLs saved to the file.
    """
    # Wayback Machine CDX API endpoint to retrieve archived URLs
    cdx_api = "http://web.archive.org/cdx/search/cdx"

    # Parameters for the API request
    params = {
        'url': target_url,  # The URL pattern to search for in the archives
        'from': start_date,  # The earliest date to search from, in YYYYMMDD format
        'to': end_date,  # The latest date to search up to, in YYYYMMDD format
        'output': 'json',  # The desired format of the output, here it's JSON
        'fl': 'original',  # The fields to return; 'original' is the URL of the archived content
        'limit': str(batch_size),  # The maximum number of results to return per batch
    }
    
    # If resuming from a specific URL, set the 'last' parameter to start from there
    if start_url:
        params['last'] = start_url
    
    total_urls = 0  # Counter to keep track of the total number of URLs retrieved
    session = requests.Session()  # Create a session for making requests (more efficient for multiple requests)

    # Open the output CSV file for writing
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["URL"])  # Write the header row in the CSV file
        
        while True:
            # Send a GET request to the Wayback Machine API with the specified parameters
            response = session.get(cdx_api, params=params)
            
            # Check if the request was successful (HTTP status code 200)
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                break

            # Parse the JSON response
            data = response.json()
            
            # If there's no data left to fetch, exit the loop
            if len(data) <= 1:
                break

            # Extract the URLs from the response data
            urls = [entry[0] for entry in data[1:]]  # Skip the header row in the data
            
            # Write the extracted URLs to the CSV file
            writer.writerows([[url] for url in urls])

            # Update the total count of URLs fetched
            total_urls += len(urls)
            print(f"Fetched and saved {total_urls} URLs so far...")

            # Update the 'last' parameter to fetch the next batch of URLs
            params['last'] = urls[-1]

    print(f"Finished fetching. Total URLs saved: {total_urls}")
    return total_urls

# Example usage
url_count = get_wayback_urls_to_csv(
    target_url="*.graaldepot.com/*",  # Search for any URL under graalonline.com and its subdomains
    start_date="19960101",   # Start date for the search (YYYYMMDD format)
    end_date="20231231",     # End date for the search (YYYYMMDD format)
    output_file="graaldepot_urls.csv",  # Name of the output CSV file
    start_url=None           # If resuming, specify the last URL fetched to start from there
)

print(f"Saved {url_count} URLs to the CSV file.")
