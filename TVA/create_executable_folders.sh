#!/bin/bash

# Create directory structure
mkdir -p executables/mac executables/windows

# Move files to appropriate directories
mv run_ocr.sh executables/mac/
mv run_ocr.command executables/mac/
mv run_ocr.scpt executables/mac/
mv run_ocr.bat executables/windows/

# Make sure Mac files are executable
chmod +x executables/mac/run_ocr.sh
chmod +x executables/mac/run_ocr.command 