import os
import io
import pathlib
import pandas as pd
from google import genai
from google.genai import types
from PyPDF2 import PdfReader
from logging_config import log

def gemini_answer(client, model, prompt, myfile):
    response = client.models.generate_content(
            model= model,   
            contents=[prompt, myfile],
            config=types.GenerateContentConfig(
                temperature= 0.8,
                thinking_config=types.ThinkingConfig(thinking_budget=512)
            )
        )
    return response

# --- Utility Functions (get_pdf_page_count remains the same) ---
def get_pdf_page_count(file_path):
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            if reader.is_encrypted:
                log.warning(f"File '{os.path.basename(file_path)}' is encrypted.")
            return len(reader.pages)
    except Exception as e:
        log.error(f"Could not read PDF file '{os.path.basename(file_path)}'. Error: {e}")
        return 0

# --- NEW, ENHANCED VALIDATION FUNCTION ---
def validate_and_correct_balances(df):
    """
    Performs a resilient, row-by-row balance check, intelligently corrects for
    shifted columns, and annotates the DataFrame with a detailed validation status for each row.

    Args:
        df (pd.DataFrame): The raw DataFrame produced by the initial parsing logic.

    Returns:
        pd.DataFrame: The corrected and annotated DataFrame, ready for export.
    """
    log.info("[Validator] Starting balance validation and correction.")
    try:
        if df.empty:
            log.warning("[Validator] Input DataFrame is empty. Skipping validation.")
            df['Validation Status'] = 'DataFrame is empty'
            return df
            
        check_df = df.copy()

        # --- Step 1: Column Shift Correction ---
        # This logic fixes cases where the ClosingBalance value was placed in the next column over.
        if 'ClosingBalance' in check_df.columns:
            closing_balance_col_index = check_df.columns.get_loc('ClosingBalance')
            
            # Check if there's at least one column after 'ClosingBalance'
            if closing_balance_col_index + 1 < len(check_df.columns):
                next_col_name = check_df.columns[closing_balance_col_index + 1]
                log.info(f"[Validator] Checking for shifted values from '{next_col_name}' to 'ClosingBalance'.")
                
                # Identify rows where the shift needs to happen
                # Condition: ClosingBalance is null/empty AND the next column has a value.
                shift_condition = check_df['ClosingBalance'].isnull() & check_df[next_col_name].notnull()
                
                if shift_condition.any():
                    # Apply the shift in a single, efficient operation
                    check_df.loc[shift_condition, 'ClosingBalance'] = check_df.loc[shift_condition, next_col_name]
                    check_df.loc[shift_condition, next_col_name] = None # Clear the old value
                    log.info(f"  > Corrected {shift_condition.sum()} shifted balance value(s).")

        # --- Step 2: Data Cleaning and Type Conversion ---
        # This is done *after* the correction to ensure we're working with the right data.
        check_df['WithdrawalAmount'] = pd.to_numeric(check_df['WithdrawalAmount'], errors='coerce').fillna(0)
        check_df['DepositAmount'] = pd.to_numeric(check_df['DepositAmount'], errors='coerce').fillna(0)
        check_df['ClosingBalance'] = pd.to_numeric(check_df['ClosingBalance'], errors='coerce')
        
        # --- Step 3: Initialization for Validation ---
        check_df['Validation Status'] = 'OK'
        
        # --- Step 4: Resilient Row-by-Row Validation Loop ---
        log.info("[Validator] Performing row-by-row balance calculation.")
        for i in range(1, len(check_df)):
            # Get values for the current and previous row
            previous_balance = check_df.loc[i-1, 'ClosingBalance']
            withdrawal = check_df.loc[i, 'WithdrawalAmount']
            deposit = check_df.loc[i, 'DepositAmount']
            reported_balance = check_df.loc[i, 'ClosingBalance']

            # Gracefully handle missing data without crashing
            if pd.isna(previous_balance):
                check_df.loc[i, 'Validation Status'] = 'Skipped: Previous balance is missing'
                continue
            if pd.isna(reported_balance):
                check_df.loc[i, 'Validation Status'] = 'Error: Closing Balance is missing or invalid'
                continue
            
            # Perform the core balance calculation
            calculated_balance = previous_balance - withdrawal + deposit
            discrepancy = calculated_balance - reported_balance
            
            # Flag any significant discrepancies
            if abs(discrepancy) > 0.01: # Using a 1-cent tolerance
                check_df.loc[i, 'Validation Status'] = f"Mismatch by {discrepancy:.2f}"
        
        # --- Step 5: Final Check on the First Row ---
        # The first row cannot be calculated, but we can check if its balance is valid.
        if not check_df.empty and pd.isna(check_df.loc[0, 'ClosingBalance']):
            check_df.loc[0, 'Validation Status'] = 'Error: Opening Balance is missing or invalid'

        log.info("[Validator] Validation complete.")
        return check_df

    except KeyError as e:
        error_msg = f"Critical Error: A required column was not found in the DataFrame -> {e}"
        log.error(f"[Validator] {error_msg}")
        df['Validation Status'] = error_msg
        return df
    except Exception as e:
        error_msg = f"An unexpected critical error occurred during validation: {e}"
        log.error(f"[Validator] {error_msg}")
        df['Validation Status'] = error_msg
        return df

# --- Main Processor Function (Using YOUR Parsing Logic) ---
def process_pdf(input_path, output_path):
    """The main PDF processing function using Gemini API."""
    try:
        log.info(f"[AI Processor] Starting processing for: {os.path.basename(input_path)}")
        
        # ... (Model Selection Logic remains the same) ...
        page_count = get_pdf_page_count(input_path)
        if page_count == 0: return "ERROR: Cannot process file with 0 pages or encrypted file."
        models = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
        current_idx = 0 if page_count >= 6 else 1
        model_to_use = models[current_idx]

        log.info(f"[AI Processor] Selected model '{model_to_use}' for {page_count} pages.")
        
        # ... (Gemini API Call Logic remains the same) ...
        client = genai.Client()
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
            response = gemini_answer(client, model_to_use,prompt,myfile)
        except Exception as e:
            log.error(e)
            if '429' or '404' in str(e):
                model_to_use = models[1 if current_idx == 0 else 0]
                response = gemini_answer(client, model_to_use,prompt,myfile) 

        client.files.delete(name=myfile.name)
        log.info(f"[AI Processor] Deleted uploaded file '{myfile.name}'.")

        if not response.text:
            return "ERROR: Model returned an empty response."
        
        # --- YOUR ROBUST PARSING LOGIC ---
        doc = io.StringIO(response.text)
        col_names = [f"col_{i}" for i in range(10)] # Read up to 10 potential columns
        df = pd.read_csv(doc, header=None, names=col_names, sep=",", engine="python", on_bad_lines='skip')
        
        # Dynamically assign column headers from the first row of data
        header_list = df.iloc[0, :len(df.columns)].fillna('Unnamed').tolist()
        
        new_columns = []
        unnamed_count = 1
        
        for col in header_list:
            if col == 'Unnamed':
                new_columns.append(f"Unnamed_{unnamed_count}")
                unnamed_count += 1
            else:
                new_columns.append(col)
        
        df.columns = new_columns
        
        df = df.iloc[1:].reset_index(drop=True)
        log.info(f"[Parser] Successfully parsed AI response into a DataFrame with {len(df.columns)} initial columns.")
        
        # --- Sort the DataFrame by Date ---
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
        df.dropna(subset=['Date'], inplace=True)
        df.sort_values(by='Date', inplace=True, kind='mergesort')
        df['Date'] = df['Date'].dt.strftime('%d/%m/%y')
        df.reset_index(drop=True, inplace=True)
        log.info("[Processor] DataFrame sorted by date.")
        
        # --- CALL THE NEW, ENHANCED VALIDATION FUNCTION ---
        validated_df = validate_and_correct_balances(df)
        log.info("[Processor] Balance correction and validation complete.")

        # --- Save Output ---
        validated_df.to_excel(output_path, index=False, engine='openpyxl')
        log.info(f"[AI Processor] Successfully created Excel file at: {output_path}")
        
        return "Success"

    except Exception as e:
        log.error(f"--- An ERROR occurred in the AI Processor: {e} ---")
        return f"ERROR: {e}"