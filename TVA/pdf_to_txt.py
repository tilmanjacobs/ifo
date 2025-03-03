import os
from pathlib import Path
import PyPDF2
import re

def clean_text(text):
    # Remove numbers that are directly connected to plus, minus signs, or underscores
    cleaned_text = re.sub(r'[\-+_]\d+|\d+[\-+_]', '', text)
    
    # Remove lowercase word strings containing triple letters
    cleaned_text = re.sub(r'\b[a-z]*([a-z])\1{2}[a-z]*\b', '', cleaned_text)
    
    # Remove lowercase word strings containing 'ee'
    cleaned_text = re.sub(r'\b[a-z]*ee[a-z]*\b', '', cleaned_text)
    
    # Remove single lowercase letters
    cleaned_text = re.sub(r'\b[a-z]\b', '', cleaned_text)
    
    # Remove two-letter lowercase strings except 'do' and 'd0'
    cleaned_text = re.sub(r'\b(?!do\b)(?!d0\b)[a-z]{2}\b', '', cleaned_text)
    
    # Remove single digits with spaces on both sides, but keep those followed by comma
    cleaned_text = re.sub(r'\s\d(?!\s*,)\s', ' ', cleaned_text)
    
    # Remove numbers containing only 2's of length 2 or greater
    cleaned_text = re.sub(r'\b2{2,}\b', '', cleaned_text)
    
    # Keep only allowed characters
    cleaned_text = re.sub(r'[^a-zA-Z0-9,.\-+_ \n]', '', cleaned_text)
    
    # Replace two or more connected periods with a minus
    cleaned_text = re.sub(r'\.{2,}', '-', cleaned_text)
    
    # Replace periods connected on both sides to minus signs with a minus
    cleaned_text = re.sub(r'-\.-', '-', cleaned_text)
    
    # Replace plus signs with minus signs
    cleaned_text = re.sub(r'\+', '-', cleaned_text)
    
    # Replace periods not preceded by capital letters or numbers with minus
    cleaned_text = re.sub(r'(?<![A-Z0-9])\.', '-', cleaned_text)
    
    # Replace underscores with minus signs
    cleaned_text = re.sub(r'_', '-', cleaned_text)
    
    # Remove spaces between minus signs
    cleaned_text = re.sub(r'-\s+-', '--', cleaned_text)
    
    # Remove periods and minus signs at the end of lines
    cleaned_text = re.sub(r'[.-]+(?=\n|$)', '', cleaned_text)
    
    # Remove strings containing numbers and multiple periods or hyphens
    cleaned_text = re.sub(r'\S*\d+\S*(?:[.\-].*[.\-])\S*', '', cleaned_text)
    
    # Keep only lines containing capital letter, number, and hyphen
    cleaned_text = '\n'.join(line for line in cleaned_text.split('\n') 
                            if re.search(r'[A-Z].*\d.*-|-.*[A-Z].*\d|\d.*[A-Z].*-', line))
    
    # Remove words with multiple capital letters
    cleaned_text = re.sub(r'\b\w*[A-Z]\w*[A-Z]\w*\b', '', cleaned_text)
    
    # Remove lowercase words containing only 'e' and consonants
    cleaned_text = re.sub(r'\b[bcdfghjklmnpqrstvwxze]+\b', '', cleaned_text)
    
    return cleaned_text

def pdf_to_text(pdf_path, output_txt):
    # Open the PDF file in read-binary mode
    with open(pdf_path, 'rb') as pdf_file:
        
        # Create a PdfReader object 
        pdf_reader = PyPDF2.PdfReader(pdf_file)

        # Initialize an empty string to store the text
        text = ''

        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
        
        # Clean the text before writing
        text = clean_text(text)

    # Write the extracted text to a text file
    with open(output_txt, 'w', encoding='utf-8') as txt_file:
        txt_file.write(text)

if __name__ == "__main__":
    # Create text directory if it doesn't exist
    if not os.path.exists('text'):
        os.makedirs('text')

    # Get all PDF files from pdf_ocr directory
    pdf_files = [f for f in os.listdir('pdf_ocr') if f.lower().endswith('.pdf')]

    for pdf_file in pdf_files:
        pdf_path = os.path.join('pdf_ocr', pdf_file)
        
        # Create output text file path with same name as PDF
        output_txt = os.path.join('text', f'{Path(pdf_file).stem}.txt')
        
        print(f'Converting {pdf_file} to text...')
        pdf_to_text(pdf_path, output_txt)
