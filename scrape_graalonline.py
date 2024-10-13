import requests

def get_wayback_urls(target_url, start_date="19960101", end_date="20210101", limit=90000000, output_file="geocities.txt"):
    """
    Fetches URLs from the Wayback Machine for a given target URL and saves them to a file.

    Args:
        target_url (str): The website URL to fetch archived URLs for.
        start_date (str): Start date in the format YYYYMMDD.
        end_date (str): End date in the format YYYYMMDD.
        limit (int): The maximum number of results to return.
        output_file (str): The file to save the URLs to.

    Returns:
        int: The number of URLs saved to the file.
    """
    cdx_api = "http://web.archive.org/cdx/search/cdx"
    params = {
        'url': target_url,
        'from': start_date,
        'to': end_date,
        'output': 'json',
        'fl': 'original',
        'limit': limit
    }
    
    response = requests.get(cdx_api, params=params, stream=True)
    if response.status_code == 200:
        url_count = 0
        with open(output_file, 'w') as file:
            for line in response.iter_lines():
                if line:  # Skip empty lines
                    url_data = line.decode('utf-8')
                    # Skip the header line
                    if url_count == 0 and url_data.startswith('original'):
                        continue
                    file.write(f"{url_data}\n")
                    url_count += 1
        return url_count
    else:
        print(f"Error: Received status code {response.status_code}")
        return 0

# Example usage
url_count = get_wayback_urls("*.geocities.com/*")
print(f"Saved {url_count} URLs to the file.")
