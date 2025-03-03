import os
import csv
from pathlib import Path

def create_empty_csv(txt_path):
    """Create an empty CSV file for the given text file."""
    # Get the filename without extension and create csv path
    csv_path = Path('csv') / f"{txt_path.stem}.csv"
    
    # Create csv directory if it doesn't exist
    csv_path.parent.mkdir(exist_ok=True)
    
    # Create empty CSV file with header
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Name', 'Title', 'Salary'])  # placeholder header

def main():
    # Get all txt files in the text directory
    text_dir = Path('text')
    if not text_dir.exists():
        print("Error: 'text' directory not found")
        return

    txt_files = list(text_dir.glob('*.txt'))
    if not txt_files:
        print("No text files found in 'text' directory")
        return

    print(f"Found {len(txt_files)} text files")
    
    # Create empty CSV files
    for txt_path in txt_files:
        create_empty_csv(txt_path)
        print(f"Created empty CSV file for {txt_path.name}")

if __name__ == "__main__":
    main() 