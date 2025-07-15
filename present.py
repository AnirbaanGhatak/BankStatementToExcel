import os
import pathlib
import time
from google import genai
from google.genai import types
import pathlib
import httpx
import pandas as pd
from dotenv import load_dotenv
import io
import shutil
from PyPDF2 import PdfReader

INPUT_FOLDER = r"\\pbserver\G\BankStatementConverter\PDF_Input"
OUTPUT_FOLDER = r"\\pbserver\G\BankStatementConverter\Excel_Output"
PROCESSED_FOLDER = r"\\pbserver\G\BankStatementConverter\Processed_pdfs"
FINAL_DESTINATION_FOLDER = r"\\pbserver\G\BankStatementConverter\Rejected_files"
SLEEP_INTERVAL = 15 # Seconds to wait between checking the input folder
FILE_SIZE_LIMIT_BYTES = 7 * 1024 * 1024


def get_pdf_page_count(file_path):
    """
    Safely gets the number of pages in a PDF file.
    Returns the page count, or 0 if the file is invalid or encrypted.
    """
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            # Check for encryption
            if reader.is_encrypted:
                print(f"--- WARNING: File '{os.path.basename(file_path)}' is encrypted and cannot be read. ---")
                return 0
            return len(reader.pages)
    except Exception as e:
        print(f"--- ERROR: Could not read PDF file '{os.path.basename(file_path)}'. Error: {e} ---")
        return 0

def validate_and_annotate_balances(df):
    """
    Performs a row-by-row balance check, annotates the DataFrame with the results,
    and flags if any discrepancies were found.

    Args:
        df (pd.DataFrame): The DataFrame extracted from the PDF.

    Returns:
        tuple: A tuple containing (pd.DataFrame, bool).
               (annotated_df, True) if all balances are correct.
               (annotated_df, False) if any discrepancy was found.
    """
    try:
        # Create a copy to avoid modifying the original DataFrame in place
        check_df = df.copy()

        # Prepare the data
        check_df['WithdrawalAmount'] = pd.to_numeric(check_df['WithdrawalAmount'], errors='coerce').fillna(0)
        check_df['DepositAmount'] = pd.to_numeric(check_df['DepositAmount'], errors='coerce').fillna(0)
        check_df['ClosingBalance'] = pd.to_numeric(check_df['ClosingBalance'], errors='coerce')

        # Create the new validation column and initialize it
        check_df['Validation Status'] = 'OK'
        
        # A flag to track if we find any errors at all

        if check_df['ClosingBalance'].isnull().any():
            check_df['Validation Status'] = 'Critical Error: Invalid Closing Balance value'
            return check_df # Return True for errors to trigger notification

        # Iterate and validate from the second row onwards
        for i in range(1, len(check_df)):
            previous_balance = check_df.loc[i-1, 'ClosingBalance']
            withdrawal = check_df.loc[i, 'WithdrawalAmount']
            deposit = check_df.loc[i, 'DepositAmount']
            reported_balance = check_df.loc[i, 'ClosingBalance']
            
            calculated_balance = previous_balance - withdrawal + deposit
            
            # Calculate the difference
            discrepancy = calculated_balance - reported_balance
            
            # If the discrepancy is larger than a small tolerance (e.g., 1 cent)
            if abs(discrepancy) > 0.01:
                # Annotate the current row with the specific error message
                check_df.loc[i, 'Validation Status'] = f"Mismatch by {discrepancy:.2f}"

        return check_df

    except Exception as e:
        # In case of a critical error during validation, create a dummy df to report it
        error_df = pd.DataFrame([{'Validation Status': f'Critical Validation Error: {e}'}])
        return error_df

def pdf_processor(input_path, output_path):

    models_gen = ["gemini-2.5-flash-preview-04-17","gemini-2.5-flash", "gemini-2.5-flash-lite-preview-06-17"]
    current_idx = 0
    try:
        cnt = get_pdf_page_count(input_path)
        if cnt >=6:
            current_idx = 0
        elif cnt <= 5:
            current_idx = 2

    except Exception as e:
        print(f"--- ERROR: {e} while geting PDF page count ---")
        return "ERROR"

    try: 
        # print(f"{API_KEY}")
        client = genai.Client()
        print("ko")
        try: 
            myfile = client.files.upload(file=input_path)
        except Exception as e:
            print(f"files ERROR: {e}")
        print("hello")
        print(f"{input_path}, {output_path}")
        prompt = """
            Extract all transactions from the provided financial document (statement or passbook) into a raw CSV string.

            **Output Schema & Headers (7 columns, exact order):**
            `Date,ChequeNo,Narration,ValueDate,WithdrawalAmount,DepositAmount,ClosingBalance`

            **Processing Rules:**
            - **Combine Multi-line Narration:** Merge multi-line transaction descriptions (like 'Particulars' or 'Narration') into a single field with spaces.
            - **Handle Missing Columns:** If a source document has no 'ValueDate', keep the column in the header but leave its data fields empty.
            - **Data Cleaning:** Remove all non-numeric characters from amount columns (e.g., '₹', ',', 'Cr', 'Dr'). `1,234.56 Cr` must become `1234.56`.
            - **Zero Values:** Represent empty or zero withdrawals/deposits as `0.00`.
            - **CRITICAL COMMA RULE:** If any field's text contains a comma, enclose that entire field in double quotes. Example: `...,"Transfer, Savings Account",...`
            - **Exclusions:** Do not include any summary or footer lines (e.g., 'Clear Balance', 'Carried Forward', 'We provide ATM Cards...').

            **Final Output:**
            - Raw CSV text only.
            - No explanations, summaries, or markdown ` ``` `.
            - Start directly with the header row.
            """

        try:
            response = client.models.generate_content(
                model=models_gen[current_idx],   
                contents=[prompt, myfile]
            )
        except Exception as e:
            if "402" in str(e):
                current_idx += 1
                if current_idx > 2:
                    current_idx = 0

        client.files.delete(name=myfile.name)

        doc = io.StringIO(response.text)

        col_names = [f"col_{i}" for i in range(10)]

        df = pd.read_csv(doc, header=None,names=col_names, sep=",",engine="python")

        df.columns = df.iloc[0, :len(df.columns)].fillna('Unnamed').tolist()
        df = df.iloc[1:].reset_index(drop=True)

        print(df.tail(5))

        validated_df = validate_and_annotate_balances(df)

        validated_df.to_excel(output_path, index=False, engine='openpyxl')

        return "Success"

    except Exception as e:
        print(f"--- An ERROR OCCURRED DURING PDF PROCESSING: {e} ---")
        return "ERROR"
    

def check_size(input_pdf_path, filename):
    try:
        file_size = os.path.getsize(input_pdf_path)
        print(f"\nValidating file: '{filename}', Size: {file_size / (1024*1024):.2f} MB")

        if file_size > FILE_SIZE_LIMIT_BYTES:
            print(f"  REJECTED: File '{filename}' is too large ({file_size / (1024*1024):.2f} MB).")
            
            # Create a new name for the rejected file
            base, ext = os.path.splitext(filename)
            rejected_filename = f"{base}_REJECTED-FILE-TOO-LARGE{ext}"
            rejected_path_final = os.path.join(FINAL_DESTINATION_FOLDER, rejected_filename)
            
            # Move the oversized file directly to the final destination
            shutil.move(input_pdf_path, rejected_path_final)

            return "rejected"
        else:
            return "continue"

    except Exception as e:
        print(f"file size error: {e}")



def main():
    """The main worker function that runs in an infinite loop."""
    print("--- Automated PDF Processor Worker Started (Mode: Two-Folder System) ---")
    print(f"Watching for files in: {INPUT_FOLDER}")
    
    # Create folders if they don't exist at startup
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    while True:
        try:
            # Get a list of all PDFs in the input folder
            pending_files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.pdf')])

            if not pending_files:
                time.sleep(SLEEP_INTERVAL)
                continue

            # Process the first file found
            filename = pending_files[0]
            print(f"\nFound new file to process: {filename}")

            # Define the full path for the files
            input_pdf_path = os.path.join(INPUT_FOLDER, filename)

            pro = check_size(input_pdf_path, filename)

            if "continue" == pro:
                basename = pathlib.Path(filename).stem
                output_filename = f"{basename}.xlsx"
                process_filename = f"{basename}_processing.pdf"
                output_pdf_path = os.path.join(OUTPUT_FOLDER, output_filename)
                input_pdf_path_pr = os.path.join(INPUT_FOLDER, process_filename)
                print(f"Starting AI processing for '{filename}'...")
                os.rename(input_pdf_path, input_pdf_path_pr)

                result_path = pdf_processor(input_pdf_path_pr, output_pdf_path)

                if "ERROR" in result_path:
                # If processing fails, the original PDF is left in the input folder
                # for manual review or another attempt.
                    print("Error")
                # We should probably wait a bit longer after a failure to avoid rapid retries on a bad file.
                    time.sleep(30)
                else:
                    shutil.move(input_pdf_path_pr, PROCESSED_FOLDER)

                    print(f"SUCCESS: Created in the output folder and moved the original PDF.")       
            else:
                print("Rejected due to size")
           

        except Exception as e:
            print(f"--- A CRITICAL ERROR OCCURRED IN THE MAIN LOOP: {e} ---")
            time.sleep(60)

if __name__ == '__main__':
    load_dotenv()
    main()