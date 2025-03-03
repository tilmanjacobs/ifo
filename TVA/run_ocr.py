import subprocess
import sys

def get_user_input():
    while True:
        response = input("Do you want to run in test mode? (y/n): ").lower()
        if response in ['y', 'n']:
            return response == 'y'
        print("Please enter 'y' for yes or 'n' for no")

def run_script(script_name, test_mode=False):
    try:
        cmd = [sys.executable, script_name]
        if test_mode:
            cmd.append("--test")
        
        print(f"\nRunning {script_name}...")
        subprocess.run(cmd, check=True)
        print(f"Successfully completed {script_name}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")
        sys.exit(1)

def main():
    test_mode = get_user_input()
    
    # Run ocr2.py first
    run_script("ocr_2.py", test_mode)
    
    # Then run pdf_to_txt.py
    run_script("pdf_to_txt.py")

if __name__ == "__main__":
    main() 