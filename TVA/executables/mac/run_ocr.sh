#!/bin/bash

# Get the directory where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set python path to use .venv from parent directory
PYTHON_PATH="$DIR/../.venv/bin/python3"

while true; do
    read -p "Do you want to run in test mode? (y/n): " response
    case $response in
        [Yy]* )
            test_mode=true
            break;;
        [Nn]* )
            test_mode=false
            break;;
        * )
            echo "Please enter 'y' for yes or 'n' for no";;
    esac
done

echo

if [ "$test_mode" = true ]; then
    echo "Running ocr_2.py in test mode..."
    "$PYTHON_PATH" ocr_2.py --test
else
    echo "Running ocr_2.py..."
    "$PYTHON_PATH" ocr_2.py
fi

if [ $? -ne 0 ]; then
    echo "Error running ocr_2.py"
    read -p "Press enter to exit"
    exit 1
fi

echo "Successfully completed ocr_2.py"
echo
echo "Running pdf_to_txt.py..."
"$PYTHON_PATH" pdf_to_txt.py

if [ $? -ne 0 ]; then
    echo "Error running pdf_to_txt.py"
    read -p "Press enter to exit"
    exit 1
fi

echo "Successfully completed pdf_to_txt.py"
echo "All scripts completed successfully"
read -p "Press enter to exit" 