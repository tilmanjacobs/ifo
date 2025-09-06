import re
import os

def get_headers():
    """
    Return the simplified headers for the CSV file.
    """
    headers = [
        "Given Names",
        "Last Name",
        "Job Title",
        "Annual Salary",
        "Hourly Rate"
    ]
    return ','.join(headers)

def split_name(name):
    """
    Split a name into given names and last name, handling both formats:
    - "First Middle Last" format
    - "Last, First Middle" format
    
    Args:
        name (str): Full name string
        
    Returns:
        tuple: (given_names, last_name)
    """
    # Clean the name first
    name = name.strip()
    
    # Check if name contains a comma (Last, First Middle format)
    if ',' in name:
        # Split on comma and reverse the order
        parts = name.split(',', 1)
        last_name = parts[0].strip()
        given_names = parts[1].strip() if len(parts) > 1 else ''
    else:
        # First Middle Last format
        parts = name.split()
        if len(parts) < 2:
            return name, ''  # Handle single name case
            
        last_name = parts[-1]
        given_names = ' '.join(parts[:-1])
    
    # Clean up any remaining punctuation in given names
    given_names = re.sub(r'[\.\-,]+', ' ', given_names)
    
    # Handle Jr., Sr., etc. in last name
    if re.search(r'\b(Jr\.?|Sr\.?|III|IV|V)\b', last_name, re.IGNORECASE):
        # Move the suffix to given names
        suffix = re.search(r'\b(Jr\.?|Sr\.?|III|IV|V)\b', last_name, re.IGNORECASE).group()
        last_name = re.sub(r'\s*\b(Jr\.?|Sr\.?|III|IV|V)\b', '', last_name, flags=re.IGNORECASE)
        given_names = f"{given_names} {suffix}"
    
    # Handle Mrs. prefix in given names
    if given_names.lower().startswith('mrs'):
        given_names = re.sub(r'^mrs\.?\s*', 'Mrs ', given_names, flags=re.IGNORECASE)
    
    # Clean up any extra spaces
    given_names = ' '.join(given_names.split())
    last_name = ' '.join(last_name.split())
    
    return given_names, last_name

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
    """
    try:
        # Read the input file
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # First remove all commas (except those in names) and dollar signs
        content = content.replace('$', '')
        
        # Split into lines and process
        lines = content.splitlines()
        
        # Process each line
        processed_lines = []
        # Add headers as first line
        processed_lines.append(get_headers())
        
        previous_job_title = ""
        
        for line in lines:
            # Skip empty lines
            if not line.strip():
                continue
                
            # Split the line into columns using multiple spaces as delimiter
            columns = re.split(r'\s{2,}', line.strip())
            
            if len(columns) >= 3:
                # Split the name into components
                given_names, last_name = split_name(columns[0])
                
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
                processed_line = f"{given_names},{last_name},{job_title},{annual},{hourly}"
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
    txt_dir = 'cleaned_text'
    for filename in os.listdir(txt_dir):
        if filename.endswith('.txt'):
            file_path = os.path.join(txt_dir, filename)
            print(f"Processing {filename}...")
            read_txt_file(file_path)

if __name__ == "__main__":
    main()
    