
import os
import requests



# Base URL pattern for the images
base_url_Asheville_NC_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-007/720357-011-007-"
base_url_Asheville_NC_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-008/720357-011-008-"
base_url_Asheville_NC_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-033/720357-033-001/720357-033-001-"
base_url_Asheville_NC_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-009/720357-011-009-"
base_url_Asheville_NC_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-002/720357-012-002-"
base_url_Asheville_NC_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-001/720357-012-001-"
base_url_Asheville_NC_7 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-032/720357-032-007/720357-032-007-"
base_url_Asheville_NC_8 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-032/720357-032-008/720357-032-008-"

base_url_Battle_Creek_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-001/720357-020-001-"
base_url_Battle_Creek_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-002/720357-020-002-"
base_url_Battle_Creek_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-004/720357-017-004-"
base_url_Battle_Creek_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-003/720357-020-003-"
base_url_Battle_Creek_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-004/720357-020-004-"
base_url_Battle_Creek_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-003/720357-017-003-"

base_url_Bay_City_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-005/720357-020-005-"
base_url_Bay_City_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-007/720357-020-007-"
base_url_Bay_City_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-006/720357-017-006-"
base_url_Bay_City_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-006/720357-020-006-"
base_url_Bay_City_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-008/720357-020-008-"
base_url_Bay_City_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-005/720357-017-005-"

base_url_Lima_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-034/720357-034-008/720357-034-008-"


# Create  directories to save the scraped images
output_dir = "/Users/luca/Library/CloudStorage/GoogleDrive-duggons@gmail.com/My Drive/Luca_Disa_research/Data/NARA_webscrape/test/"


# Function to download an image given its URL and page number
def download_image(city,page_number):
    ##  Add the create folder part
    
    output_dir_city = f"{output_dir}{city}"
    if not os.path.exists(output_dir_city):
        os.makedirs(output_dir_city)
    
    # Format the page number to always be 4 digits (e.g., 0001, 0002, etc.)
    page_str = f"{page_number:04}"
    
    url_name = f"base_url_{city}"
    img_url = globals().get(url_name)

    try:
        # Send a request to the image URL
        img_data = requests.get(img_url).content
        
        # Define the filename and path for saving the image
        img_filename = f"image_{page_str}.jpg"
        img_path = os.path.join(output_dir_city, img_filename)
        
        # Save the image
        with open(img_path, 'wb') as handler:
            handler.write(img_data)
        
        print(f"Downloaded: {img_filename}")
    except Exception as e:
        print(f"Failed to download image for page {page_str}: {e}")

# Loop through the page numbers from 1 to 1978 and download each image
for page_no in range(1, end_range_city):
    download_image("Asheville_NC_1", page_no)





    import requests
import os
import pandas as pd

# Read the Excel file


# Base URL pattern for the images
base_url_Asheville_NC_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-007/720357-011-007-"
base_url_Asheville_NC_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-008/720357-011-008-"
base_url_Asheville_NC_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-033/720357-033-001/720357-033-001-"
base_url_Asheville_NC_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-011/720357-011-009/720357-011-009-"
base_url_Asheville_NC_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-002/720357-012-002-"
base_url_Asheville_NC_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-001/720357-012-001-"
base_url_Asheville_NC_7 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-032/720357-032-007/720357-032-007-"
base_url_Asheville_NC_8 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-032/720357-032-008/720357-032-008-"

base_url_Battle_Creek_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-001/720357-020-001-"
base_url_Battle_Creek_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-002/720357-020-002-"
base_url_Battle_Creek_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-004/720357-017-004-"
base_url_Battle_Creek_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-003/720357-020-003-"
base_url_Battle_Creek_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-004/720357-020-004-"
base_url_Battle_Creek_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-003/720357-017-003-"

base_url_Bay_City_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-005/720357-020-005-"
base_url_Bay_City_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-007/720357-020-007-"
base_url_Bay_City_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-006/720357-017-006-"
base_url_Bay_City_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-006/720357-020-006-"
base_url_Bay_City_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-020/720357-020-008/720357-020-008-"
base_url_Bay_City_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-017/720357-017-005/720357-017-005-"

base_url_Lima_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-034/720357-034-008/720357-034-008-"


# Create  directories to save the scraped images
output_dir = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/scraped_images/"


# Function to download an image given its URL and page number
def download_image(city,page_number):
    ##  Add the create folder part
    
    output_dir_city = f"{output_dir}{city}"
    if not os.path.exists(output_dir_city):
        os.makedirs(output_dir_city)
    
    # Format the page number to always be 4 digits (e.g., 0001, 0002, etc.)
    page_str = f"{page_number:04}"
    
    img_str = f"{page_str}.tif"

    url_name = f"base_url_{city}"
    img_url = globals().get(url_name)
    img_url = f"{img_url}{img_str}"

    try:
        # Send a request to the image URL
        img_data = requests.get(img_url).content
        
        # Define the filename and path for saving the image
        img_filename = f"image_{page_str}.tif"
        img_path = os.path.join(output_dir_city, img_filename)
        
        # Save the image
        with open(img_path, 'wb') as handler:
            handler.write(img_data)
        
        print(f"Downloaded: {img_filename}")
    except Exception as e:
        print(f"Failed to download image for page {page_str}: {e}")

# Loop through the page numbers from 1 to 1978 and download each image
for city in cities:
    for doc_no in range(1, 8):
        # Check if base URL exists for this city and doc number
        url_name = f"base_url_{city}_{doc_no}"
        if url_name not in globals():
            print(f"No URL found for {url_name}, skipping...")
            continue
            
        page_no = 1
        while True:
            try:
                download_image(f"{city}_{doc_no}", page_no)
                page_no += 1
            except Exception as e:
                print(f"Stopped at page {page_no} due to error: {e}")
                break