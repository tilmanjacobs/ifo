#brew install unpaper

import ocrmypdf
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
import shutil

# Set up logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

def process_pdf(pdf_path: Path, backup_dir: Path) -> None:
    """Process a single PDF file with OCR in place."""
    try:
        # Create backup
        backup_path = backup_dir / f"{pdf_path.stem}_original{pdf_path.suffix}"
        shutil.copy2(pdf_path, backup_path)
        
        logging.info(f"Processing: {pdf_path.name}")
        ocrmypdf.ocr(
            pdf_path,
            pdf_path,  # Same path for input and output
            skip_text=True,  # Skip PDFs that already contain text
            deskew=True,    # Straighten pages
            clean=True,     # Clean pages from scanning artifacts
            optimize=1      # Basic optimization
        )
        logging.info(f"Completed: {pdf_path.name}")
    except Exception as e:
        logging.error(f"Error processing {pdf_path.name}: {str(e)}")
        # Restore from backup in case of error
        shutil.copy2(backup_path, pdf_path)
        logging.info(f"Restored original file for {pdf_path.name}")

def process_folder(folder_path: str, max_workers: int = 4) -> None:
    """Process all PDFs in the folder using multiple threads."""
    folder_path = Path(folder_path)
    backup_dir = folder_path / "originals"
    backup_dir.mkdir(exist_ok=True)
    
    pdf_files = list(folder_path.glob("*.pdf"))
    
    if not pdf_files:
        logging.warning(f"No PDF files found in {folder_path}")
        return
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for pdf_file in pdf_files:
            if pdf_file.parent != backup_dir:  # Skip files in backup directory
                executor.submit(process_pdf, pdf_file, backup_dir)

if __name__ == "__main__":
    # Example usage
    folder_path = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/pdfs"
    process_folder(folder_path) 