import os
import io
import pathlib
import pandas as pd
from google import genai
from google.genai import types
from PyPDF2 import PdfReader
from logging_config import log

# This function can be part of a utility module if you prefer
def get_pdf_page_count(file_path):
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            if reader.is_encrypted:
                log.warning(f"File '{os.path.basename(file_path)}' is encrypted.")
                return 0
            return len(reader.pages)
    except Exception as e:
        log.error(f"Could not read PDF file '{os.path.basename(file_path)}'. Error: {e}")
        return 0

def validate_and_annotate_balances(df):
    try:
        if df.empty:
            df['Validation Status'] = 'DataFrame is empty'
            return df
            
        # Create a copy to avoid modifying the original DataFrame in place
        check_df = df.copy()

        # --- Data Cleaning and Type Conversion ---
        # Convert amount columns to numeric, coercing errors to NaN (Not a Number)
        check_df['WithdrawalAmount'] = pd.to_numeric(check_df['WithdrawalAmount'], errors='coerce')
        check_df['DepositAmount'] = pd.to_numeric(check_df['DepositAmount'], errors='coerce')
        check_df['ClosingBalance'] = pd.to_numeric(check_df['ClosingBalance'], errors='coerce')
        
        # Fill any resulting NaNs in transaction columns with 0, as they represent no transaction
        check_df['WithdrawalAmount'] = check_df['WithdrawalAmount'].fillna(0)
        check_df['DepositAmount'] = check_df['DepositAmount'].fillna(0)

        # --- Initialization ---
        # Create the validation column and initialize it to 'OK' by default
        check_df['Validation Status'] = 'OK'
        
        # --- Row-by-Row Validation Loop ---
        # Iterate from the second row (index 1) to the end of the DataFrame
        for i in range(1, len(check_df)):
            # Get the balance from the *previous* row
            previous_balance = check_df.loc[i-1, 'ClosingBalance']
            
            # Get the transaction amounts and reported balance for the *current* row
            withdrawal = check_df.loc[i, 'WithdrawalAmount']
            deposit = check_df.loc[i, 'DepositAmount']
            reported_balance = check_df.loc[i, 'ClosingBalance']

            # --- Resilient Logic ---
            # Check 1: Is the previous balance a valid number? If not, we can't calculate.
            if pd.isna(previous_balance):
                check_df.loc[i, 'Validation Status'] = 'Skipped: Previous balance is missing'
                continue # Move to the next row

            # Check 2: Is the current reported balance a valid number? If not, flag it.
            if pd.isna(reported_balance):
                check_df.loc[i, 'Validation Status'] = 'Error: Closing Balance is missing or invalid'
                continue # Move to the next row
            
            # If all numbers are valid, perform the calculation
            calculated_balance = previous_balance - withdrawal + deposit
            
            # Calculate the difference between what we calculated and what the bank reported
            discrepancy = calculated_balance - reported_balance
            
            # Check 3: Is the discrepancy significant? (Using a small tolerance for floating point math)
            if abs(discrepancy) > 0.01:
                # Annotate the current row with the specific mismatch amount
                check_df.loc[i, 'Validation Status'] = f"Mismatch by {discrepancy:.2f}"
            
            # If the discrepancy is within the tolerance, the status remains 'OK'
        
        # Handle the very first row - we can't validate it, but we can check if it's a valid number.
        if pd.isna(check_df.loc[0, 'ClosingBalance']):
            check_df.loc[0, 'Validation Status'] = 'Error: Opening Balance is missing or invalid'

        return check_df

    except KeyError as e:
        # Handle cases where expected columns are missing from the input DataFrame
        error_msg = f'Critical Error: Missing expected column -> {e}'
        log.error(error_msg)
        df['Validation Status'] = error_msg
        return df
    except Exception as e:
        # Catch any other unexpected errors during the process
        error_msg = f'Critical Validation Error: {e}'
        log.error(error_msg)
        df['Validation Status'] = error_msg
        return df

def process_pdf(input_path, output_path):
    """The main PDF processing function using Gemini API."""
    try:
        log.info(f"[AI Processor] Starting processing for: {os.path.basename(input_path)}")
        
        # --- Model Selection Logic ---
        page_count = get_pdf_page_count(input_path)
        if page_count == 0:
            return "ERROR: Cannot process file with 0 pages or encrypted file."
        
        models = ["gemini-2.5-flash", "gemini-2.5-flash-lite-preview-06-17"]
        model_to_use = models[0] if page_count >= 6 else models[1]
        log.info(f"[AI Processor] Selected model '{model_to_use}' for {page_count} pages.")
        
        # --- Gemini API Interaction ---
        client = genai.Client()
        log.info("[AI Processor] Authenticated with Gemini API.")
        
        myfile = client.files.upload(file=input_path)
        log.info(f"[AI Processor] File '{myfile.name}' uploaded.")

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
                model=model_to_use,
                contents=[prompt, myfile],
                # Add other config as needed
            )
        except Exception as e:
            log.warning(f"[AI Processor] API call with '{model_to_use}' failed: {e}. Trying fallback model.")
            # Simple fallback logic
            fallback_model = models[1] if model_to_use == models[0] else models[0]
            log.info(f"[AI Processor] Retrying with model '{fallback_model}'.")
            response = client.models.generate_content(
                model=fallback_model,
                contents=[prompt, myfile]
            )

        client.files.delete(name=myfile.name)
        log.info(f"[AI Processor] Deleted uploaded file '{myfile.name}'.")

        # --- Data Parsing and Validation ---
        if not response.text:
            return "ERROR: Model returned an empty response."
        
        csv_text_io = io.StringIO(response.text)
        # Assuming the first row is always the header
        df = pd.read_csv(csv_text_io, sep=",", engine="python", on_bad_lines='skip')
        log.info(f"[AI Processor] Parsed response into a DataFrame with {len(df)} rows.")
        log.info(df.tail)

        validated_df = validate_and_annotate_balances(df)
        log.info("[AI Processor] Balance validation complete.")
        log.info(df.tail)


        # --- Save Output ---
        validated_df.to_excel(output_path, index=False, engine='openpyxl')
        log.info(f"[AI Processor] Successfully created Excel file at: {output_path}")
        
        return "Success"

    except Exception as e:
        log.error(f"--- An ERROR occurred in the AI Processor: {e} ---")
        # Attempt to delete the file from Gemini even on failure
        try:
            if 'myfile' in locals() and myfile:
                client.files.delete(name=myfile.name)
        except Exception as delete_e:
            log.error(f"Could not clean up uploaded file on error: {delete_e}")
            
        return f"ERROR: {e}"