import fitz
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def compress_pdf(input_path: Path, output_path: Path, compression_level: int = 2):
    """
    Compress a PDF file by removing unnecessary data
    Level 1: Basic cleanup
    Level 2: More aggressive cleanup
    Level 3: Maximum cleanup
    """
    try:
        original_size = input_path.stat().st_size
        doc = fitz.open(input_path)

        # Common cleanup parameters
        params = {
            "deflate": True,     # Compress streams
            "garbage": 4,        # Maximum garbage collection
            "clean": True,       # Remove unnecessary elements
            "ascii": False,      # Don't convert binary to ASCII
            "expand": False,     # Don't decompress images
            "no_new_id": True,   # Don't create new IDs
        }

        # Save with compression
        doc.save(output_path, **params)
        doc.close()
        
        # Show results
        compressed_size = output_path.stat().st_size
        ratio = (1 - compressed_size / original_size) * 100
        
        logging.info(
            f"Compressed {input_path.name}: "
            f"{original_size/1024/1024:.2f}MB → "
            f"{compressed_size/1024/1024:.2f}MB "
            f"({ratio:.1f}% reduction)"
        )
        
    except Exception as e:
        logging.error(f"Error compressing {input_path.name}: {str(e)}")

def process_folder(folder_path: str, compression_level: int = 2):
    """Process all PDFs in the folder"""
    folder_path = Path(folder_path)
    output_folder = folder_path / "compressed"
    output_folder.mkdir(exist_ok=True)
    
    pdf_files = list(folder_path.glob("*.pdf"))
    
    if not pdf_files:
        logging.warning(f"No PDF files found in {folder_path}")
        return
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        for pdf_file in pdf_files:
            if pdf_file.parent != output_folder:
                output_path = output_folder / pdf_file.name
                executor.submit(compress_pdf, pdf_file, output_path, compression_level)

# Settings
FOLDER_PATH = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/pdfs"  # Change this to your folder path
COMPRESSION_LEVEL = 1  # Compression level is kept for compatibility but not used

if __name__ == "__main__":
    process_folder(FOLDER_PATH, COMPRESSION_LEVEL) 