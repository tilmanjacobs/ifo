import requests
import os
import pandas as pd
import img2pdf
import glob

# Read the Excel file
# Read the Excel file and extract city names from first column, skipping header row
df = pd.read_excel('data_availability.xlsx')
cities = [city.replace(", ", "_").replace(" ", "_") for idx, city in enumerate(df.iloc[1:, 0].tolist()) if df.iloc[idx+1, 1] == 1]

# Create directories to save the scraped images and PDFs
output_dir = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/scraped_images/"
pdf_dir = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/pdfs/"

# Create both directories if they don't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
if not os.path.exists(pdf_dir):
    os.makedirs(pdf_dir)

def create_pdf_from_tifs(folder_path):
    """
    Creates a single PDF from all TIF files in the specified folder
    """
    try:
        # Get all .tif files in the folder and sort them
        tif_files = sorted(glob.glob(os.path.join(folder_path, "*.tif")))
        
        if not tif_files:
            print(f"No TIF files found in {folder_path}")
            return
        
        # Create PDF filename from folder name
        folder_name = os.path.basename(folder_path)
        pdf_path = os.path.join(pdf_dir, f"{folder_name}.pdf")
        
        # Convert TIFs to PDF
        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert(tif_files))
        
        print(f"Created PDF: {pdf_path}")
            
    except Exception as e:
        print(f"Error creating PDF: {e}")

def download_image(city, page_number):
    """
    Downloads an image given its URL and page number
    """
    output_dir_city = f"{output_dir}{city}"
    if not os.path.exists(output_dir_city):
        os.makedirs(output_dir_city)
    
    # Format the page number to always be 4 digits
    page_str = f"{page_number:04}"
    img_str = f"{page_str}.tif"

    url_name = f"base_url_{city}"
    img_url = globals().get(url_name)
    img_url = f"{img_url}{img_str}"

    try:
        response = requests.get(img_url)
        response.raise_for_status()
        
        if response.status_code == 200 and len(response.content) > 0:
            img_filename = f"image_{page_str}.tif"
            img_path = os.path.join(output_dir_city, img_filename)
            
            with open(img_path, 'wb') as handler:
                handler.write(response.content)
            
            print(f"Downloaded: {img_filename}")
            return True
        else:
            print(f"No valid image found for page {page_str}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"Failed to download image for page {page_str}: {e}")
        return False


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

base_url_Durham_NC_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-033/720357-033-004/720357-033-004-"
base_url_Durham_NC_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-008/720357-012-008-"
base_url_Durham_NC_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-033/720357-033-005/720357-033-005-"
base_url_Durham_NC_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-007/720357-012-007-"
base_url_Durham_NC_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-010/720357-012-010-"
base_url_Durham_NC_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-009/720357-012-009-"

base_url_Elmira_NY_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-016/720357-016-007/720357-016-007-"
base_url_Elmira_NY_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-016/720357-016-008/720357-016-008-"


base_url_Greensboro_NC_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-012/720357-012-011/720357-012-011-"
base_url_Greensboro_NC_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-013/720357-013-001/720357-013-001-"
base_url_Greensboro_NC_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-013/720357-013-002/720357-013-002-"
base_url_Greensboro_NC_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-013/720357-013-003/720357-013-003-"

base_url_Hamilton_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-034/720357-034-007/720357-034-007-"

base_url_Jackson_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-018/720357-018-004/720357-018-004-"
base_url_Jackson_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-006/720357-006-001/720357-006-001-"

base_url_Jackson_MS_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-001/720357-001-005/720357-001-005-"

base_url_Jamestown_NY_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-016/720357-016-009/720357-016-009-"
base_url_Jamestown_NY_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-016/720357-016-010/720357-016-010-"

base_url_Kalamazoo_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-018/720357-018-005/720357-018-005-"
base_url_Kalamazoo_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-006/720357-006-005/720357-006-005-"
base_url_Kalamazoo_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-018/720357-018-006/720357-018-006-"
base_url_Kalamazoo_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-006/720357-006-003/720357-006-003-"
base_url_Kalamazoo_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-006/720357-006-004/720357-006-004-"
base_url_Kalamazoo_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-006/720357-006-002/720357-006-002-"

base_url_Lancaster_PA_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-032/720357-032-006/720357-032-006-"
base_url_Lancaster_PA_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-026/720357-026-010/720357-026-010-"

base_url_Lorain_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-035/720357-035-001/720357-035-001-"
base_url_Lorain_OH_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-024/720357-024-008/720357-024-008-"

base_url_Muskegon_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-019/720357-019-002/720357-019-002-"
base_url_Muskegon_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-019/720357-019-001/720357-019-001-"
base_url_Muskegon_MI_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-007/720357-007-003/720357-007-003-"
base_url_Muskegon_MI_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-007/720357-007-006/720357-007-006-"
base_url_Muskegon_MI_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-007/720357-007-004/720357-007-004-"
base_url_Muskegon_MI_6 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-007/720357-007-005/720357-007-005-"

base_url_New_Castle_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-008/720357-008-007/720357-008-007-"
base_url_New_Castle_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-008/720357-008-008/720357-008-008-"

base_url_Perth_Amboy_MI_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-015/720357-015-002/720357-015-002-"
base_url_Perth_Amboy_MI_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-015/720357-015-001/720357-015-001-"

base_url_Portsmouth_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-024/720357-024-009/720357-024-009-"

base_url_Pueblo_CO_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-039/720357-039-006/720357-039-006-"

base_url_Saint_Petersburg_FL_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-036/720357-036-003/720357-036-003-"
base_url_Saint_Petersburg_FL_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-036/720357-036-002/720357-036-002-"
base_url_Saint_Petersburg_FL_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-036/720357-036-005/720357-036-005-"
base_url_Saint_Petersburg_FL_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-036/720357-036-004/720357-036-004-"
base_url_Saint_Petersburg_FL_5 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-036/720357-036-006/720357-036-006-"

base_url_San_Jose_CA_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-004/720357-038-004-"
base_url_San_Jose_CA_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-001/720357-038-001-"
base_url_San_Jose_CA_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-002/720357-038-002-"
base_url_San_Jose_CA_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-003/720357-038-003-"

base_url_Stamford_CT_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-028/720357-028-008/720357-028-008-"
base_url_Stamford_CT_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-028/720357-028-005/720357-028-005-"
base_url_Stamford_CT_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-028/720357-028-006/720357-028-006-"
base_url_Stamford_CT_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-028/720357-028-007/720357-028-007-"

base_url_Stockton_CA_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-008/720357-038-008-"
base_url_Stockton_CA_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-005/720357-038-005-"
base_url_Stockton_CA_3 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-006/720357-038-006-"
base_url_Stockton_CA_4 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-038/720357-038-007/720357-038-007-"

base_url_Warren_OH_1 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-025/720357-025-003/720357-025-003-"
base_url_Warren_OH_2 = "https://s3.amazonaws.com/NARAprodstorage/lz/dc-metro/rg-195/720357/720357-025/720357-025-004/720357-025-004-"



# Main processing loop
for city in cities:
    for doc_no in range(1,9):
        city_doc = f"{city}_{doc_no}"
        # Check if base URL exists for this city_doc
        if f"base_url_{city_doc}" not in globals():
            print(f"No base URL found for {city_doc}, skipping...")
            continue
            
        print(f"\nProcessing {city_doc}...")
        
        # Download all pages for this document
        page_no = 1
        downloaded_pages = False
        while True:
            try:
                success = download_image(city_doc, page_no)
                if not success:
                    break
                downloaded_pages = True
                print(f"Downloaded: {city_doc} - page {page_no:04}")
                page_no += 1
            except Exception as e:
                print(f"Stopped at page {page_no} due to error: {e}")
                break
        
        # Create PDF for this document's folder after downloading all images
        folder_path = os.path.join(output_dir, city_doc)
        if downloaded_pages and os.path.exists(folder_path):
            print(f"\nCreating PDF for {city_doc}")
            create_pdf_from_tifs(folder_path)
            print(f"Completed processing {city_doc}\n")
        elif not downloaded_pages:
            print(f"No pages were downloaded for {city_doc}")

