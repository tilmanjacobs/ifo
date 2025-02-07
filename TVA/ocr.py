import os
from pathlib import Path
from pdf2image import convert_from_path
import pytesseract

# Define input and output directories
input_dir = Path("pdf")
output_dir = Path("pdf_ocr")
output_dir.mkdir(exist_ok=True)

def process_pdf(pdf_path):
    # Convert PDF to images with higher DPI
    images = convert_from_path(pdf_path, dpi=300)
    
    # Create text file path (same name as PDF but with .txt extension)
    output_path = output_dir / f"{pdf_path.stem}.txt"
    
    # Process each page and append to text file
    with open(output_path, 'w', encoding='utf-8') as text_file:
        for i, image in enumerate(images):
            # Perform OCR with specific configuration for multi-column
            custom_config = r'--psm 3 --oem 3 -c textord_min_linesize=3 preserve_interword_spaces=1'
            text = pytesseract.image_to_string(
                image,
                lang='eng',
                config=custom_config
            )
            
            # Write page number and content
            text_file.write(f"\n\n--- Page {i+1} ---\n\n")
            text_file.write(text)

def main():
    # Get all PDFs and sort them
    pdf_files = sorted(list(input_dir.glob('*.pdf')))
    
    # Process all PDFs in the input directory
    for pdf_file in pdf_files:
        print(f"Processing: {pdf_file.name}")
        try:
            process_pdf(pdf_file)
            print(f"Successfully processed: {pdf_file.name}")
        except Exception as e:
            print(f"Error processing {pdf_file.name}: {str(e)}")

if __name__ == "__main__":
    main()
