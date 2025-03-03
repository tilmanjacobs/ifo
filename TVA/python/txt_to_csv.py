import os
import csv
from pathlib import Path
import re
import json
from datetime import datetime

def process_line(line):
    """Process a line by splitting on 3 or more hyphens and cleaning up the data."""
    # First remove any commas
    line = line.replace(",", "")
    
    # First try splitting on 3 or more hyphens
    parts = [part.strip() for part in re.split(r'-{3,}', line)]
    
    # If we don't get exactly 3 parts, try splitting on 2 or more hyphens
    if len(parts) != 3:
        parts = [part.strip() for part in re.split(r'-{2,}', line)]
    
    # If still not 3 parts, try to find the first number
    if len(parts) != 3:
        # Find the first number in the line
        number_match = re.search(r'\d+', line)
        if number_match:
            number_pos = number_match.start()
            # Split the text before the number on the last hyphen sequence
            text_before = line[:number_pos].strip()
            name_title = [part.strip() for part in re.split(r'-{2,}', text_before)]
            
            if len(name_title) >= 2:
                parts = [
                    name_title[0],
                    name_title[1],
                    line[number_pos:].strip()
                ]
    
    # Clean up parts if we have them
    if len(parts) == 3:
        # Replace any remaining dashes with spaces
        parts = [part.replace('-', ' ') for part in parts]
        # Remove spaces between numbers in the last part (salary)
        parts[2] = re.sub(r'\s+(?=\d)', '', parts[2])
        parts[2] = re.sub(r'(?<=\d)\s+', '', parts[2])
        
        # Split the name into last name and given name
        full_name = parts[0].strip()
        name_parts = full_name.split(maxsplit=1)
        last_name = name_parts[0]
        given_name = name_parts[1] if len(name_parts) > 1 else ""
        
        # Reconstruct parts with separate last name and given name
        parts = [last_name, given_name, parts[1], parts[2]]
    
    # Debug print
    print(f"Debug - parts after split: {parts}")
    
    if len(parts) != 4:  # Now expecting 4 parts
        print(f"Skipping malformed line: {line}")
        return None
        
    return parts

def convert_txt_to_csv(txt_path):
    """Convert text file to CSV format."""
    csv_path = Path('csv') / f"{txt_path.stem}.csv"
    csv_path.parent.mkdir(exist_ok=True)
    
    valid_rows = []
    skipped_count = 0
    with open(txt_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:  # Skip empty lines
                processed = process_line(line)
                if processed:
                    valid_rows.append(processed)
                else:
                    skipped_count += 1
    
    print(f"Processed {txt_path.name}: {len(valid_rows)} valid rows, {skipped_count} skipped")
    
    # Update header for CSV
    header = ['Last Name', 'Given Name', 'Title', 'Salary']

    # Write to CSV
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(valid_rows)

def get_target_years():
    """Get target years from user input, filtering out non-numeric characters except commas."""
    history_file = 'year_history.json'
    
    # Try to read previous years from JSON file
    try:
        with open(history_file, 'r') as f:
            history = json.load(f)
            previous_years = history['years']
            print(f"Previous years used: {', '.join(previous_years)}")
            print(f"Last used: {history['last_used']}")
            
            add_more = input("Would you like to add more years? (y/n): ").lower()
            if add_more == 'y':
                user_input = input("Enter additional years (comma-separated): ")
            else:
                # Update last used timestamp
                history['last_used'] = datetime.now().isoformat()
                with open(history_file, 'w') as f:
                    json.dump(history, f, indent=2)
                return previous_years
    except (FileNotFoundError, json.JSONDecodeError):
        user_input = input("Enter target years (comma-separated): ")
        previous_years = []

    # Keep only numbers and commas
    filtered_input = ''.join(char for char in user_input if char.isdigit() or char == ',')
    # Split by comma and filter out empty strings
    years = [year.strip() for year in filtered_input.split(',') if year.strip()]
    
    if years:
        # Combine with previous years and remove duplicates
        all_years = sorted(list(set(previous_years + years)))
        
        # Save years to JSON file with timestamp
        history = {
            'years': all_years,
            'last_used': datetime.now().isoformat()
        }
        with open(history_file, 'w') as f:
            json.dump(history, f, indent=2)
        return all_years
        
    print("Please enter at least one valid year.")
    return get_target_years()

def main():
    # Get specific txt files from the text directory
    text_dir = Path('text')
    if not text_dir.exists():
        print("Error: 'text' directory not found")
        return

    target_years = get_target_years()
    txt_files = [
        txt_file 
        for txt_file in text_dir.glob('*.txt')
        if any(year in txt_file.stem for year in target_years)
    ]

    if not txt_files:
        print("No text files found containing target years in 'text' directory")
        return

    print(f"Processing {len(txt_files)} text files")
    
    # Convert text files to CSV
    for txt_path in txt_files:
        convert_txt_to_csv(txt_path)
        print(f"Converted {txt_path.name} to CSV")

if __name__ == "__main__":
    main() 