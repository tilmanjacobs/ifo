import requests
import os
import pandas as pd
from bs4 import BeautifulSoup

# Read the Excel file
# Read the Excel file and extract city names from first column, skipping header row
df = pd.read_excel('data_availability.xlsx')
cities = [city.replace(", ", "_") for city in df.iloc[1:, 0].tolist()]
# After reading the Excel file, get the URLs from rows 2-9
urls = df.iloc[1:9, 1].tolist()  # Assuming the links are in the second column

def get_download_link(url):
    try:
        # Send GET request to the webpage
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the download button/link
        # You might need to adjust this selector based on the actual HTML structure
        download_button = soup.find('a', {'class': 'download'})  # or whatever class/id the button has
        
        if download_button and 'href' in download_button.attrs:
            full_link = download_button['href']
            # Split the URL by '/' and take only the first 9 parts
            parts = full_link.split('/')[:9]
            # Join them back together with '/'
            truncated_link = '/'.join(parts) + '/'
            return truncated_link
        else:
            return None
            
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return None

# Get download links for each URL
download_links = []
for url in urls:
    link = get_download_link(url)
    if link:
        download_links.append(link)
    print(f"Found download link: {link}")

# Store the links (optional)
# You can save them to a new Excel file or use them directly
download_df = pd.DataFrame({'Download Links': download_links})
download_df.to_excel('download_links.xlsx', index=False)
