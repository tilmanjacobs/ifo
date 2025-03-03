import os
from pdf2image import convert_from_path
import pytesseract
from io import BytesIO
from PyPDF2 import PdfMerger
import cv2
import numpy as np
from PIL import Image
import json

# ---------------------------
# CONFIGURATION
# ---------------------------

# Test mode flag - if True, only processes first page of first PDF
TEST_MODE = True

# Define the input and output directories
input_dir = 'pdf'
output_dir = 'pdf_ocr'

# Create the output directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Add these constants after other configuration settings
PARAMS_FILE = 'denoising_params.json'

# Set custom Tesseract configuration options.
# --oem 3: Use the default OCR Engine Mode
# --psm 6: Assume a uniform block of text (better for tables)
# -c preserve_interword_spaces=1: Maintains spacing between words
# -c tessedit_do_invert=0: Don't invert colors
custom_config = r'--oem 3 --psm 6 -c preserve_interword_spaces=1 -c tessedit_do_invert=1'

def load_saved_params():
    """Load previously saved denoising parameters"""
    try:
        with open(PARAMS_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_params(params):
    """Save denoising parameters for future use"""
    with open(PARAMS_FILE, 'w') as f:
        json.dump(params, f, indent=4)

def get_denoising_params(pdf_files):
    """Get denoising parameters for each PDF, either from saved values or user input"""
    saved_params = load_saved_params()
    current_params = saved_params.copy()
    
    # Check if there are any saved parameters for the current files
    saved_files = set(saved_params.keys()) & set(pdf_files)
    if saved_files:
        print("\nFound saved parameters for the following files:")
        for file in saved_files:
            print(f"  {file}: h={saved_params[file]}")
        
        update = input("\nWould you like to update any of these parameters? (y/N): ").lower()
        if update != 'y':
            # Only prompt for new files
            pdf_files = [f for f in pdf_files if f not in saved_params]
    
    # Process remaining files that need parameters
    for pdf_file in pdf_files:
        while True:
            try:
                h_value = int(input(f"Enter denoising parameter (h) for {pdf_file} (10-60, higher = more denoising): "))
                if 1 <= h_value <= 100:  # reasonable range for h parameter
                    current_params[pdf_file] = h_value
                    break
                else:
                    print("Please enter a value between 1 and 100")
            except ValueError:
                print("Please enter a valid number")
    
    # Save the parameters for future use
    save_params(current_params)
    return current_params

def preprocess_image(image, h_param=45):
    """Preprocess the image to improve OCR accuracy."""
    # Convert PIL Image to cv2 format
    opencv_image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    
    # Convert to grayscale
    gray = cv2.cvtColor(opencv_image, cv2.COLOR_BGR2GRAY)
    
    # Apply stronger non-local means denoising
    # h: filter strength (higher = more denoising). Default is 3
    # templateWindowSize: size of template patch. Default is 7
    # searchWindowSize: size of window for searching similar patches. Default is 21
    denoised = cv2.fastNlMeansDenoising(gray, h=h_param, templateWindowSize=7, searchWindowSize=21)
    
    # Convert back to PIL Image
    return Image.fromarray(denoised)

# ---------------------------
# PROCESSING THE PDF FILES
# ---------------------------

# List all PDF files in the input folder
pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
if not pdf_files:
    print("No PDF files found in the 'pdf' folder.")
    exit(0)

if TEST_MODE:
    print("Running in TEST MODE - processing first page of each PDF file")

# Get denoising parameters for each PDF
denoising_params = get_denoising_params(pdf_files)

for pdf_file in pdf_files:
    input_pdf_path = os.path.join(input_dir, pdf_file)
    output_pdf_path = os.path.join(output_dir, f'ocr_test_{pdf_file}' if TEST_MODE else pdf_file)
    print(f'Processing "{pdf_file}" ...')

    try:
        # Convert the PDF pages to high-resolution images.
        # A DPI of 300 is generally a good starting point for OCR.
        pages = convert_from_path(input_pdf_path, dpi=300)
        # In test mode, only process the first page
        if TEST_MODE:
            pages = pages[:1]
    except Exception as e:
        print(f'Error converting "{pdf_file}" to images: {e}')
        continue

    # Initialize a PdfMerger to combine OCR'd pages into one PDF
    merger = PdfMerger()

    for i, page in enumerate(pages, start=1):
        print(f'  OCR processing page {i}...')
        try:
            # Use the stored parameter for this PDF
            h_param = denoising_params[pdf_file]
            
            # Preprocess the page image with custom h parameter
            processed_page = preprocess_image(page, h_param=h_param)
            
            # Convert the processed page image into a PDF with an OCR text layer
            pdf_bytes = pytesseract.image_to_pdf_or_hocr(
                processed_page, extension='pdf', config=custom_config
            )
            # Wrap the bytes in a BytesIO object so PyPDF2 can read it.
            pdf_stream = BytesIO(pdf_bytes)
            # Append this page to the merger.
            merger.append(pdf_stream)
        except Exception as e:
            print(f'    Error processing page {i} of "{pdf_file}": {e}')
            continue

    try:
        # Write the merged PDF (with the new OCR layer) to the output folder.
        with open(output_pdf_path, 'wb') as f_out:
            merger.write(f_out)
        merger.close()
        print(f'Finished processing "{pdf_file}". Saved new OCR PDF to "{output_pdf_path}".\n')
    except Exception as e:
        print(f'Error writing output PDF for "{pdf_file}": {e}')
