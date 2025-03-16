from mistralai import Mistral
import os
import pandas as pd
import numpy as np

api_key = os.environ["MISTRAL_API_KEY"]
client = Mistral(api_key=api_key)

# Create text directory if it doesn't exist
os.makedirs('text', exist_ok=True)
os.makedirs('csv', exist_ok=True)

# Process the PDF using Mistral OCR with a document URL
ocr_response = client.ocr.process(
    model="mistral-ocr-latest",
    document={
        "type": "document_url",
        "document_url": "https://drive.google.com/uc?export=download&id=1F96aSOBRj1jD7p0EaLskxUY_JeAdA2bv"
    },
    include_image_base64=True
)

# Create output files
output_file = "text/salaries_1943_mistral.txt"
csv_file = "csv/salaries_1943_mistral.csv"

def clean_salary(salary_str):
    """Clean salary string by handling both period and comma formats"""
    # Remove $ and spaces
    cleaned = salary_str.replace('$', '').replace(' ', '')
    
    # Handle period-formatted numbers (e.g., 1.971.00)
    if cleaned.count('.') > 1:
        cleaned = cleaned.replace('.', '')
        if len(cleaned) > 2:  # Add decimal point for cents
            cleaned = cleaned[:-2] + '.' + cleaned[-2:]
    else:
        # Handle regular comma format
        cleaned = cleaned.replace(',', '')
    
    return float(cleaned)

# Function to convert markdown table to pandas DataFrame
def markdown_to_df(markdown_table):
    lines = markdown_table.strip().split('\n')
    # Remove the separator line (contains |------|)
    lines = [line for line in lines if '|-' not in line]
    # Split each line into columns and clean up
    data = []
    for line in lines:
        row = [cell.strip() for cell in line.strip('|').split('|')]
        # Ensure consistent number of columns
        if data and len(row) != len(data[0]):
            # If this row has more columns, merge extra columns
            if len(row) > len(data[0]):
                merged_value = ' '.join(row[len(data[0])-1:])
                row = row[:len(data[0])-1] + [merged_value]
            # If this row has fewer columns, pad with empty strings
            else:
                row.extend([''] * (len(data[0]) - len(row)))
        data.append(row)
    
    if data:
        return pd.DataFrame(data[1:], columns=data[0])
    return None

# Function to round down to nearest 10
def round_down_to_10(x):
    return np.floor(x / 10) * 10

# List to store all employee DataFrames
all_employee_dfs = []

# Write text output and process tables
with open(output_file, 'w') as f:
    # Process each page
    for page_num, page in enumerate(ocr_response.pages):
        print(f"Processing page {page_num + 1}...")
        f.write(f"\n\nPage {page_num + 1}\n")
        f.write("="*80 + "\n\n")
        
        # Split the text into sections based on the table headers
        sections = page.markdown.split('\n\n')
        
        # Process and write each table in the page
        for i, section in enumerate(sections):
            if '|' in section:  # Check if section contains a table
                # Extract table title (text before the table)
                title = sections[i-1] if i > 0 and '|' not in sections[i-1] else "Table"
                f.write(f"Title: {title}\n\n")
                
                # Convert to DataFrame and write to file
                df = markdown_to_df(section)
                if df is not None:
                    f.write(df.to_string(index=False))
                    f.write("\n\n")
                    
                    # If this is an employee table, add it to our collection
                    if "Name" in df.columns and "Title" in df.columns and "Salary per annum" in df.columns:
                        try:
                            # Clean up salary data with custom function
                            df['Salary per annum'] = df['Salary per annum'].apply(clean_salary)
                            # Round down salaries to nearest 10
                            df['Salary per annum'] = df['Salary per annum'].apply(round_down_to_10)
                            # Add page number for reference
                            df['Page'] = page_num + 1
                            all_employee_dfs.append(df)
                        except Exception as e:
                            print(f"Error processing salary data on page {page_num + 1}: {e}")
                            print(f"Problematic row data: {df[df['Salary per annum'].apply(lambda x: not isinstance(x, (int, float)))]}")

    # Write additional information
    f.write("\nOCR Processing Info:\n")
    f.write(f"Pages processed: {ocr_response.usage_info.pages_processed}\n")
    f.write(f"Document size: {ocr_response.usage_info.doc_size_bytes/1024:.2f} KB\n")
    f.write(f"Model used: {ocr_response.model}\n")

# Combine all employee DataFrames and save to CSV
if all_employee_dfs:
    final_df = pd.concat(all_employee_dfs, ignore_index=True)
    final_df.to_csv(csv_file, index=False)
    
    # Print summary statistics
    print(f"\nProcessed {len(ocr_response.pages)} pages")
    print(f"Found {len(final_df)} employee records")
    print("\nSalary Statistics:")
    print(f"Average Salary: ${final_df['Salary per annum'].mean():.2f}")
    print(f"Highest Salary: ${final_df['Salary per annum'].max():.2f}")
    print(f"Lowest Salary: ${final_df['Salary per annum'].min():.2f}")
    print(f"\nResults saved to:")
    print(f"- Full text: {output_file}")
    print(f"- Employee data: {csv_file}")
else:
    print("No employee data found in the document")
