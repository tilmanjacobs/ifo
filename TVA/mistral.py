from mistralai import Mistral
import os
import requests
from bs4 import BeautifulSoup
import re

api_key = os.environ["MISTRAL_API_KEY"]
client = Mistral(api_key=api_key)

# Create text directory if it doesn't exist
os.makedirs('text', exist_ok=True)

# GitHub repository information
repo_owner = "tilmanjacobs"
repo_name = "ifo"
path = "TVA/pdf"

# Function to get list of PDF files from GitHub
def get_pdf_files_from_github(owner, repo, path):
    url = f"https://github.com/{owner}/{repo}/tree/main/{path}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    pdf_files = set()
    for item in soup.find_all('a', href=True):
        href = item['href']
        if href.endswith('.pdf') and f"{owner}/{repo}/blob/main/{path}" in href:
            filename = href.split('/')[-1]
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/main/{path}/{filename}"
            pdf_files.add((filename, raw_url))
    
    return pdf_files

# Get list of PDF files
pdf_files = get_pdf_files_from_github(repo_owner, repo_name, path)
print(f"Found {len(pdf_files)} PDF files")

# Process each PDF file
for filename, url in pdf_files:
    print(f"Processing {filename}...")
    
    # Extract year from filename (assuming format like salaries_1938.pdf)
    year_match = re.search(r'(\d{4})', filename)
    year = year_match.group(1) if year_match else "unknown"
    
    # Create output filename
    output_file = f"text/{os.path.splitext(filename)[0]}_mistral.txt"
    
    # Process the PDF using Mistral OCR
    ocr_response = client.ocr.process(
        model="mistral-ocr-latest",
        document={
            "type": "document_url",
            "document_url": url
        }
    )
    
    # Write text output
    with open(output_file, 'w') as f:
        # Process each page
        for page_num, page in enumerate(ocr_response.pages):
            f.write(f"\n\nPage {page_num + 1}\n")
            f.write("="*80 + "\n\n")
            f.write(page.markdown)
        
        # Write additional information
        f.write("\n\nOCR Processing Info:\n")
        f.write(f"Pages processed: {ocr_response.usage_info.pages_processed}\n")
        f.write(f"Document size: {ocr_response.usage_info.doc_size_bytes/1024:.2f} KB\n")
        f.write(f"Model used: {ocr_response.model}\n")
    
    print(f"Saved text to {output_file}")

print("\nAll PDF files processed successfully!")
