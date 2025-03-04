import re
from pathlib import Path
import enchant  # for English word checking

def clean_salary_table(input_text):
    # Find the section with employee data (after Table III)
    table_split = input_text.split("TABLE III.—")
    if len(table_split) < 2:
        return []
    
    employee_section = table_split[1]
    
    # Initialize English dictionary
    d = enchant.Dict("en_US")
    
    # Split into lines and clean them
    lines = employee_section.split('\n')
    cleaned_data = []
    
    # First cleanup: remove any combination of -.+_ longer than 2 characters
    pattern_cleanup = r'[-_.+]{3,}'
    
    for line in lines:
        # Skip empty lines and obvious headers
        if not line.strip() or 'TABLE' in line:
            continue
        
        # Clean up the line
        cleaned_line = re.sub(pattern_cleanup, ' ', line)
        
        # Split the line by spaces
        words = cleaned_line.split()
        
        # Skip if less than 3 words (likely not a valid entry)
        if len(words) < 3:
            continue
        
        # Keep only words that are either:
        # 1. In English dictionary
        # 2. Start with capital letter (likely names)
        # 3. Are numbers (with possible commas)
        filtered_words = []
        for word in words:
            word = word.strip('.,|:;(){}[]')
            if (d.check(word) or 
                (word and word[0].isupper()) or 
                bool(re.match(r'^[\d,]+$', word))):
                filtered_words.append(word)
        
        # Try to extract name, title, and salary
        if len(filtered_words) >= 3:
            # Find the last number in the list (salary)
            salary_indices = [i for i, word in enumerate(filtered_words) 
                            if bool(re.match(r'^[\d,]+$', word))]
            
            if salary_indices:
                salary_idx = salary_indices[-1]
                salary = filtered_words[salary_idx].replace(',', '')
                
                # Name is typically the first 1-3 words
                name_end = min(3, salary_idx)
                name = ' '.join(filtered_words[:name_end])
                
                # Title is everything between name and salary
                title = ' '.join(filtered_words[name_end:salary_idx])
                
                if name and title and salary:
                    cleaned_data.append({
                        'name': name,
                        'title': title,
                        'salary': salary
                    })
    
    return cleaned_data

def process_file(input_path, output_path):
    # Read input file
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Clean the data
    cleaned_data = clean_salary_table(content)
    
    # Write output file
    with open(output_path, 'w', encoding='utf-8') as f:
        for entry in cleaned_data:
            f.write(f"{entry['name']}; {entry['title']}; {entry['salary']}\n")

def main():
    # Get all txt files in the text directory
    input_dir = Path("text")
    output_dir = Path("text/cleaned")
    output_dir.mkdir(exist_ok=True)
    
    for txt_file in input_dir.glob("*.txt"):
        if txt_file.name.startswith("cleaned_"):
            continue
            
        output_path = output_dir / f"cleaned_{txt_file.name}"
        print(f"Processing: {txt_file.name}")
        
        try:
            process_file(txt_file, output_path)
            print(f"Successfully cleaned: {txt_file.name}")
        except Exception as e:
            print(f"Error processing {txt_file.name}: {str(e)}")

if __name__ == "__main__":
    main()
