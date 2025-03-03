#!/bin/bash

# Get the directory where this script is located and move up two levels (out of executables/TVA)
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$DIR/../../.."
TVA_DIR="$DIR/../.."

# Set python path to use .venv from parent directory of TVA
PYTHON_PATH="$PARENT_DIR/.venv/bin/python3"

# Change to TVA directory where the Python scripts are
cd "$TVA_DIR"

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
echo
echo "Running txt_to_csv.py..."
"$PYTHON_PATH" txt_to_csv.py

if [ $? -ne 0 ]; then
    echo "Error running txt_to_csv.py"
    read -p "Press enter to exit"
    exit 1
fi

echo "Successfully completed txt_to_csv.py"
echo "All scripts completed successfully"
read -p "Press enter to exit" 