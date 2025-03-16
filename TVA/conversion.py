import re
import os

def get_headers():
    """
    Return the headers for the CSV file.
    
    Returns:
        str: Comma-separated headers
    """
    headers = [
        "First Name",
        "Middle Name 1",
        "Middle Name 2",
        "Middle Name 3",
        "Last Name",
        "Job Title",
        "Annual Salary",
        "Hourly Rate"
    ]
    return ','.join(headers)

#def split_name(name):
    """
    Split a name into first, middle names, and last name.
    Handles 'Mrs.' prefix as first name.
    
    Args:
        name (str): Full name string
        
    Returns:
        str: Name components joined by commas
    """
    # Clean the name first
    name = re.sub(r'[\s\.\-,]+$', '', name.strip())
    
    # Check for Mrs. prefix
    if name.lower().startswith('mrs'):
        # Remove any dots after Mrs and split
        name = re.sub(r'^mrs\.?\s*', 'Mrs,', name, flags=re.IGNORECASE)
        parts = name.split(',')[1].strip().split()
    else:
        parts = name.split()
    
    # Handle the rest of the name parts
    if name.lower().startswith('mrs'):
        first_name = 'Mrs'
        remaining_parts = parts  # Keep all parts after Mrs
    else:
        first_name = parts[0]
        remaining_parts = parts[1:]  # Keep all remaining parts as middle/last names
        
        # Only split first name if it contains periods (initials)
        if '.' in first_name:
            initials = [p.strip() for p in first_name.split('.') if p.strip()]
            first_name = initials[0]
            # Add any additional initials to the beginning of remaining_parts
            remaining_parts = initials[1:] + remaining_parts
    
    # Initialize middle names and last name
    middle_name1 = remaining_parts[0] if len(remaining_parts) > 1 else ''
    middle_name2 = remaining_parts[1] if len(remaining_parts) > 2 else ''
    middle_name3 = remaining_parts[2] if len(remaining_parts) > 3 else ''
    last_name = remaining_parts[-1] if remaining_parts else ''
    
    # If there's only one remaining part after Mrs., it's the last name
    if name.lower().startswith('mrs') and len(remaining_parts) == 1:
        middle_name1 = middle_name2 = middle_name3 = ''
        last_name = remaining_parts[0]
    # If there's only two parts total (not counting Mrs.), no middle names
    elif len(remaining_parts) == 2:
        middle_name1 = middle_name2 = middle_name3 = ''
        last_name = remaining_parts[1]
    
    return f"{first_name},{middle_name1},{middle_name2},{middle_name3},{last_name}"

def clean_column(text, preserve_spaces=False):
    """
    Clean trailing punctuation and spaces from column text.
    
    Args:
        text (str): Column text to clean
        preserve_spaces (bool): Whether to preserve internal spaces
        
    Returns:
        str: Cleaned text
    """
    # Remove trailing punctuation and spaces
    text = re.sub(r'[\s\.\-,]+$', '', text.strip())
    
    # Remove all spaces if not preserving them
    if not preserve_spaces:
        text = text.replace(' ', '')
        
    return text

def is_do_variant(job_title):
    """
    Check if job title is a variant of 'do' with special characters.
    
    Args:
        job_title (str): Job title to check
        
    Returns:
        bool: True if it's a variant of 'do', False otherwise
    """
    # Clean the title for checking
    cleaned = re.sub(r'[.\-,+_\s]+', '', job_title.lower())
    return cleaned == 'do'

def assign_salary(value):
    """
    Assign salary to appropriate column based on value.
    If > 10, goes to annual salary, if < 10, goes to hourly rate.
    
    Args:
        value (str): Salary value
        
    Returns:
        tuple: (annual_salary, hourly_rate)
    """
    if not value:
        return "", ""
        
    try:
        num_value = float(value)
        if num_value > 10:
            return value, ""
        else:
            return "", value
    except ValueError:
        return "", ""

def read_txt_file(file_path):
    """
    Read content from a text file and convert to CSV format.
    Splits the first column into name components.
    Preserves spaces in second column.
    Assigns salary to appropriate column based on value.
    Saves output to csv directory at same level as txt folder.
    
    Args:
        file_path (str): Path to the text file
    """
    try:
        # Read the input file
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # First remove all commas and dollar signs from the entire content
        content = content.replace(',', '').replace('$', '')
        
        # Split into lines and process
        lines = content.splitlines()
        
        # Process each line
        processed_lines = []
        # Add headers as first line
        processed_lines.append(get_headers())
        
        previous_job_title = ""  # Store previous job title
        
        for line in lines:
            # Replace 2 or more spaces with a single comma
            processed_line = re.sub(r'\s{2,}', ',', line.strip())
            
            # Split the line into columns
            columns = processed_line.split(',')
            if len(columns) >= 3:
                # Split the name into components
                name_columns = split_name(columns[0])
                
                # Clean job title and handle 'do' replacement
                job_title = clean_column(columns[1], preserve_spaces=True)
                if is_do_variant(job_title):
                    job_title = previous_job_title
                else:
                    previous_job_title = job_title
                
                # Clean and assign salary value to appropriate column
                salary_value = clean_column(columns[2], preserve_spaces=False) if len(columns) > 2 else ""
                annual, hourly = assign_salary(salary_value)
                
                # Combine all columns
                processed_line = f"{name_columns},{job_title},{annual},{hourly}"
                
            processed_lines.append(processed_line)
            
        # Get parent directory of txt folder and create csv directory there
        txt_dir = os.path.dirname(file_path)
        parent_dir = os.path.dirname(txt_dir)
        csv_dir = os.path.join(parent_dir, 'csv')
        os.makedirs(csv_dir, exist_ok=True)
        
        # Generate output filename in csv directory
        base_name = os.path.basename(file_path)
        csv_name = os.path.splitext(base_name)[0] + '.csv'
        csv_path = os.path.join(csv_dir, csv_name)
        
        # Remove existing file if it exists
        try:
            if os.path.exists(csv_path):
                os.remove(csv_path)
        except PermissionError:
            print(f"Error: Unable to overwrite '{csv_path}'. Please check file permissions.")
            return
            
        # Save the new CSV file
        try:
            with open(csv_path, 'w', encoding='utf-8') as csv_file:
                csv_file.write('\n'.join(processed_lines))
        except PermissionError:
            print(f"Error: Unable to create '{csv_path}'. Please check file permissions.")
            return
            
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Error processing file: {str(e)}")

def main():
    # Get all txt files in the txt directory
    txt_dir = 'txt'
    for filename in os.listdir(txt_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(txt_dir, filename)
            print(f"Processing {filename}...")
            read_txt_file(file_path)

if __name__ == "__main__":
    main()
    