import os
import re

# Create cleaned text directory if it doesn't exist
os.makedirs('cleaned_text', exist_ok=True)

# Get all OCR text files
text_files = [f for f in os.listdir('text') if f.endswith('_mistral.txt')]
print(f"Found {len(text_files)} OCR text files to clean")

# Process each text file
for filename in text_files:
    print(f"Cleaning {filename}...")
    input_path = os.path.join('text', filename)
    output_path = os.path.join('cleaned_text', filename.replace('_mistral.txt', '_cleaned.txt'))
    
    rows_deleted = 0
    deleted_rows = []
    
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        found_total = False
        buffer_lines = []
        processed_lines = []
        
        for line in infile:
            # Remove any comma that's adjacent to a number before any other processing
            line = re.sub(r'(?:\d,|,\d)', lambda m: m.group().replace(',', ''), line)
            
            # Replace 1.1215 with 1.12
            line = re.sub(r'1\.1215\b', '1.12', line)
            
            # Delete periods that are preceded by any single digit and followed by three numbers
            line = re.sub(r'(\d)\.(\d{3})', r'\1\2', line)
            
            # Check if line contains required elements
            has_letter = bool(re.search(r'[a-zA-Z]', line))
            has_number = bool(re.search(r'\d', line))
            has_period_or_comma = bool(re.search(r'[.,]', line))
            
            # Skip lines that don't meet all criteria
            if not (has_letter and has_number and has_period_or_comma):
                continue
            
            # Find all numbers in the line (including those with periods/commas)
            numbers = re.findall(r'\d+(?:[,.]\d+)*', line)
            
            # Skip lines with multiple numbers
            if len(numbers) > 1:
                continue
            
            # Check if current line contains "Total" or "total"
            if re.search(r'total', line, re.IGNORECASE):
                found_total = True
                buffer_lines = []  # Clear the buffer
                continue  # Skip the line with "Total"
            
            if found_total:
                # Process lines after "Total" was found
                cleaned_text = re.sub(r'[^a-zA-Z0-9,.\s]', ' ', line)
                cleaned_text = re.sub(r'\.+', '.', cleaned_text)
                cleaned_text = re.sub(r' {3,}', ' ' * 15, cleaned_text)
                cleaned_text = cleaned_text.lstrip()
                
                # Skip lines that contain the word "Name" (case-insensitive)
                if re.search(r'Name', cleaned_text, re.IGNORECASE):
                    continue
                    
                # Then skip lines that start with "Names" or "Table"
                if cleaned_text.strip().startswith("Names") or cleaned_text.strip().startswith("Table"):
                    continue
                    
                # Clean the text to keep only allowed characters (including spaces and linebreaks)
                cleaned_text = re.sub(r'[^a-zA-Z0-9,.\s]', ' ', line)
                # Remove multiple periods (keeping only single periods)
                cleaned_text = re.sub(r'\.+', '.', cleaned_text)
                
                # Replace any string of three or more spaces with 15 spaces
                cleaned_text = re.sub(r' {3,}', ' ' * 15, cleaned_text)
                
                # Store the cleaned line for later filtering
                processed_lines.append((cleaned_text, line.strip()))
            else:
                # Store lines in buffer until we find "Total"
                buffer_lines.append(line)
        
        # If "Total" was never found, process all lines normally
        if not found_total:
            for line in buffer_lines:
                cleaned_text = re.sub(r'[^a-zA-Z0-9,.\s]', ' ', line)
                cleaned_text = re.sub(r'\.+', '.', cleaned_text)
                cleaned_text = re.sub(r' {3,}', ' ' * 15, cleaned_text)
                cleaned_text = cleaned_text.lstrip()
                
                # Skip lines that contain the word "Name" (case-insensitive)
                if re.search(r'Name', cleaned_text, re.IGNORECASE):
                    continue
                    
                # Then skip lines that start with "Names" or "Table"
                if cleaned_text.strip().startswith("Names") or cleaned_text.strip().startswith("Table"):
                    continue
                    
                # Clean the text to keep only allowed characters (including spaces and linebreaks)
                cleaned_text = re.sub(r'[^a-zA-Z0-9,.\s]', ' ', line)
                # Remove multiple periods (keeping only single periods)
                cleaned_text = re.sub(r'\.+', '.', cleaned_text)
                
                # Replace any string of three or more spaces with 15 spaces
                cleaned_text = re.sub(r' {3,}', ' ' * 15, cleaned_text)
                
                # Store the cleaned line for later filtering
                processed_lines.append((cleaned_text, line.strip()))
        
        # Now filter out lines with multiple numbers after all other cleaning is done
        for cleaned_text, original_line in processed_lines:
            # Replace period in pattern like "1.23." with "0"
            cleaned_text = re.sub(r'(\d)\.(\d{2})\.' , r'\g<1>0\g<2>.', cleaned_text)
            
            # Check if line contains multiple numbers
            # This regex finds numbers that must contain a digit and may include commas and periods
            numbers = re.findall(r'[0-9]+(?:[,.][0-9]+)*', cleaned_text)
            
            # Count only numbers that contain a digit AND a period
            valid_numbers = [num for num in numbers if '.' in num]
            
            if len(valid_numbers) > 1:
                rows_deleted += 1
                deleted_rows.append(original_line)
                continue  # Skip this line
            
            # Write the line that passed all filters
            outfile.write(cleaned_text)
    
    print(f"Saved cleaned text to {output_path}")
    print(f"Deleted {rows_deleted} rows with multiple numbers from {filename}:")
    for row in deleted_rows:
        print(f"  - {row}")
    print()

print("\nAll text files cleaned successfully!") 