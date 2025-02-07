#!/usr/bin/env python3
import os
from pdf2image import convert_from_path
import pytesseract
from io import BytesIO
from PyPDF2 import PdfMerger

# ---------------------------
# CONFIGURATION
# ---------------------------

# Test mode flag - if True, only processes first page of first PDF
TEST_MODE = False

# Define the input and output directories
input_dir = 'pdf'
output_dir = 'pdf_ocr'

# Create the output directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# Set custom Tesseract configuration options.
# --oem 3: Use the default OCR Engine Mode
# --psm 6: Assume a uniform block of text (better for tables)
# -c preserve_interword_spaces=1: Maintains spacing between words
# -c tessedit_do_invert=0: Don't invert colors
custom_config = r'--oem 3 --psm 6 -c preserve_interword_spaces=1 -c tessedit_do_invert=0'

# ---------------------------
# PROCESSING THE PDF FILES
# ---------------------------

# List all PDF files in the input folder
if TEST_MODE:
    test_file = 'test.pdf'
    if not os.path.exists(os.path.join(input_dir, test_file)):
        print(f"Test mode enabled but '{test_file}' not found in the '{input_dir}' folder.")
        exit(0)
    pdf_files = [test_file]
    print(f"Running in TEST MODE - only processing first page of {test_file}")
else:
    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    if not pdf_files:
        print("No PDF files found in the 'pdf' folder.")
        exit(0)

for pdf_file in pdf_files:
    input_pdf_path = os.path.join(input_dir, pdf_file)
    output_pdf_path = os.path.join(output_dir, 'ocr_test.pdf' if TEST_MODE else pdf_file)
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
            # Convert the page image into a PDF with an OCR text layer.
            # The output is in PDF format (as bytes).
            pdf_bytes = pytesseract.image_to_pdf_or_hocr(page, extension='pdf', config=custom_config)
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
