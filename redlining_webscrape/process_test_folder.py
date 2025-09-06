import os
import glob
import img2pdf
import ocrmypdf

# Define directories
base_dir = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/"
input_dir = os.path.join(base_dir, "scraped_images/Asheville_NC_5")
pdf_dir = os.path.join(base_dir, "pdfs")

# Create PDF directory if it doesn't exist
if not os.path.exists(pdf_dir):
    os.makedirs(pdf_dir)

def create_pdf_from_tifs(folder_path):
    """
    Creates a single PDF from all TIF files in the specified folder and makes it searchable
    """
    try:
        # Get all .tif files in the folder and sort them
        tif_files = sorted(glob.glob(os.path.join(folder_path, "*.tif")))
        
        if not tif_files:
            print(f"No TIF files found in {folder_path}")
            return
        
        # Create PDF filename from folder name
        folder_name = os.path.basename(folder_path)
        temp_pdf_path = os.path.join(pdf_dir, f"temp_{folder_name}.pdf")
        final_pdf_path = os.path.join(pdf_dir, f"{folder_name}.pdf")
        
        # Convert TIFs to PDF first
        with open(temp_pdf_path, "wb") as f:
            f.write(img2pdf.convert(tif_files))
        
        print(f"Created initial PDF: {temp_pdf_path}")
        
        # Now make the PDF searchable with OCR
        try:
            ocrmypdf.ocr(
                temp_pdf_path, 
                final_pdf_path,
                language='eng',
                deskew=True,
                force_ocr=True,
                progress_bar=True
            )
            print(f"Created searchable PDF: {final_pdf_path}")
            
            # Remove temporary PDF
            os.remove(temp_pdf_path)
            
        except Exception as e:
            print(f"OCR failed: {str(e)}")
            # If OCR fails, keep the non-searchable PDF
            os.rename(temp_pdf_path, final_pdf_path)
            
    except Exception as e:
        print(f"Error creating PDF: {e}")

if __name__ == "__main__":
    if os.path.exists(input_dir):
        print(f"Processing test folder: {input_dir}")
        create_pdf_from_tifs(input_dir)
    else:
        print(f"Test folder not found: {input_dir}")