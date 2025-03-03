# OCR Script Executables

This folder contains executable scripts for running the OCR processing tools on different operating systems.

## Windows Users

1. Navigate to the `executables/windows` folder
2. Double-click `run_ocr.bat` to run the script

## Mac Users

1. Navigate to the `executables/mac` folder
2. First time setup (only needed once):
   - Open Terminal
   - Navigate to this directory:
     ```bash
     cd path/to/project/executables/mac
     ```
   - Set the required permissions:
     ```bash
     chmod +x run_ocr.sh
     chmod +x run_ocr.command
     ```
3. To run the script:
   - Double-click `run_ocr.command`
   - Or run `./run_ocr.sh` in Terminal

## What the Scripts Do

These scripts will:
1. Ask if you want to run in test mode
2. Run ocr_2.py with your chosen mode
3. Run pdf_to_txt.py
4. Show progress and any error messages
5. Wait for you to press Enter before closing

## Troubleshooting

### Mac Permission Errors
If you see "Permission denied" errors:
1. Open Terminal
2. Navigate to the mac executables folder
3. Run the chmod commands shown above

### Python Environment Issues
Make sure you have:
1. Python installed
2. The virtual environment (`.venv`) set up in the parent directory
3. All required packages installed in the virtual environment 