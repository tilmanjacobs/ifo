import os
import glob
import img2pdf
from PIL import Image
import numpy as np

# Define directories
base_dir = "/Users/tilmanjacobs/Documents/ifo/redlining_webscrape/"
input_dir = os.path.join(base_dir, "scraped_images")
pdf_dir = os.path.join(base_dir, "pdfs")

# Specific folders to process
folders_to_process = [
    "Asheville_NC_1",
    "Asheville_NC_2",
    "Asheville_NC_3"
]

def convert_to_8bit(img):
    """
    Convert high bit-depth images to 8-bit
    """
    max_value = float(2 ** 16 - 1) if img.mode == 'I' else 255.0
    img_array = np.array(img)
    img_8bit = ((img_array / max_value) * 255).astype(np.uint8)
    return Image.fromarray(img_8bit)

def create_pdf_from_tifs(folder_path):
    """
    Creates a single PDF from all TIF files in the specified folder,
    converting images only if necessary
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
        
        print(f"\nProcessing {folder_name}...")
        print(f"Found {len(tif_files)} TIF files")
        
        # Convert images if needed and prepare for PDF
        images_for_pdf = []
        temp_files = []
        
        for i, tif_file in enumerate(tif_files, 1):
            try:
                with Image.open(tif_file) as img:
                    needs_conversion = False
                    
                    # Check if conversion is needed
                    if img.mode in ['I', 'F']:  # High bit-depth
                        print(f"Image {i}: Converting from high bit-depth ({img.mode})")
                        needs_conversion = True
                        img = convert_to_8bit(img)
                    
                    if img.mode not in ['RGB', 'L']:  # Not in RGB or Grayscale
                        print(f"Image {i}: Converting color space from {img.mode}")
                        needs_conversion = True
                        img = img.convert('RGB')
                    
                    if needs_conversion:
                        # Save converted image
                        temp_path = tif_file.replace('.tif', '_converted.tif')
                        img.save(temp_path, 'TIFF', compression='tiff_deflate')
                        images_for_pdf.append(temp_path)
                        temp_files.append(temp_path)
                    else:
                        # Use original image
                        images_for_pdf.append(tif_file)
                        print(f"Image {i}: No conversion needed")
                    
                print(f"Processed image {i}/{len(tif_files)}")
            except Exception as e:
                print(f"Error processing {tif_file}: {e}")
                continue
        
        # Create PDF from images
        try:
            with open(pdf_path, "wb") as f:
                f.write(img2pdf.convert(images_for_pdf))
            print(f"Successfully created PDF: {pdf_path}")
        except Exception as e:
            print(f"Error creating PDF: {e}")
        
        # Clean up temporary files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except:
                pass
            
    except Exception as e:
        print(f"Error processing folder {folder_path}: {e}")

def main():
    # Create PDF directory if it doesn't exist
    if not os.path.exists(pdf_dir):
        os.makedirs(pdf_dir)
    
    # Process each folder in the list
    for folder_name in folders_to_process:
        folder_path = os.path.join(input_dir, folder_name)
        
        if os.path.exists(folder_path):
            print(f"\nStarting conversion for {folder_name}")
            create_pdf_from_tifs(folder_path)
        else:
            print(f"\nFolder not found: {folder_name}")

if __name__ == "__main__":
    print("Starting PDF conversion process...")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {pdf_dir}")
    print(f"Folders to process: {', '.join(folders_to_process)}")
    
    main()
    
    print("\nConversion process completed!") 