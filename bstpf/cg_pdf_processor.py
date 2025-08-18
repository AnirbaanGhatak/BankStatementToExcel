import os
import io
import pathlib
import pandas as pd
from google import genai
from google.genai import types
from PyPDF2 import PdfReader
from logging_config import log
import numpy as np

# ==============================================================================
# 1. CONFIGURATION & PROMPTS
# ==============================================================================

# --- Paste your two final prompt strings here ---

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

CAMS_PROMPT = """
    Your task: Process a CAMS Capital Gains report. Extract all transaction-level data into a single, clean CSV.

**Extraction Rules:**

1.  **Determine `Transaction_Type` by analyzing the `Scheme Name`:**
    *   If the name contains `Equity`, `Cap`, `Contra`, `Value`, `Thematic`, `Dividend`, `Focused`, or `Opportunities` -> set as **`EQUITY_MF`**.
    *   Otherwise, it's a debt fund. Check its `Date_of_Purchase`:
        *   Before 2023-04-01 -> set as **`DEBT_MF_INDEXED`**.
        *   On or after 2023-04-01 -> set as **`DEBT_MF_SLAB_RATE`**.

2.  **Map data from the report to the CSV columns:**
    *   `Scheme Name` -> `Particulars`
    *   `ISIN` -> `ISIN_Code`
    *   `Redeemed Units` -> `Quantity`
    *   `Purchase` date -> `Date_of_Purchase`
    *   `Redemption` date -> `Date_of_Transfer`
    *   `Amount` (from Redemption line) -> `Sale_Consideration`
    *   (Calculate: `Redeemed Units * Unit Cost`) -> `Actual_Cost_of_Acquisition`
    *   **CRITICAL:** `Market value as on 31/01/2018` -> `FMV_on_31012018`
    *   (Calculate: Sum of `Short Term` + `Long Term` gains) -> `Abs_Gain_Loss`
    *   (Calculate: Days between dates) -> `Holding_Days`

 **Final Output:**
        - Raw CSV text only.
        - No explanations, summaries, or markdown ` ``` `.
        - Start directly with the header row.

    Transaction_Type,Particulars,ISIN_Code,Quantity,Date_of_Purchase,Date_of_Transfer,Sale_Consideration,Selling_Expenses,Net_Sale_Consideration,Actual_Cost_of_Acquisition,Indexed_Cost,FMV_on_31012018,Abs_Gain_Loss,Holding_Days
   

"""

GENERAL_PROMPT = """
Your task is to act as an expert data extractor. Convert all transactions into a single, clean CSV file with no extra text.

**Instructions:**

1.  **Classify `Transaction_Type` based on the instrument type AND its purchase date:**
    *   `EQUITY_MF`: For stocks and all equity mutual funds.
    *   `DEBT_MF_INDEXED`: For debt/liquid funds with a `Date_of_Purchase` **BEFORE 2023-04-01**.
    *   `DEBT_MF_SLAB_RATE`: For debt/liquid funds with a `Date_of_Purchase` **ON OR AFTER 2023-04-01**.
    *   `OTHER_NON_EQUITY`: For Gold funds, international funds, etc.
    *   `VDA`: For crypto or Virtual Digital Assets.

2.  **Extract data into these specific CSV columns.** If a value is missing, leave the field blank.
    *   `Transaction_Type` (from Rule 1)
    *   `Particulars`, `ISIN_Code`, `Quantity`, `Date_of_Purchase`, `Date_of_Transfer`
    *   `Sale_Consideration`, `Selling_Expenses`, `Net_Sale_Consideration`
    *   `Actual_Cost_of_Acquisition`, `Indexed_Cost`
    *   `FMV_on_31012018` (**CRITICAL**: Find 'Value/NAV as on 31-01-2018')
    *   `Abs_Gain_Loss` (Find 'Abs.(G/L)')
    *   `Holding_Days`

**Final Output:**
        - Raw CSV text only.
        - No explanations, summaries, or markdown ` ``` `.
        - Start directly with the header row.

Transaction_Type,Particulars,ISIN_Code,Quantity,Date_of_Purchase,Date_of_Transfer,Sale_Consideration,Selling_Expenses,Net_Sale_Consideration,Actual_Cost_of_Acquisition,Indexed_Cost,FMV_on_31012018,Abs_Gain_Loss,Holding_Days
"""

# ==============================================================================
# 2. DATA EXTRACTION & PARSING FUNCTION
# ==============================================================================

def process_capital_gains_report(file_path, prompt_to_use):
    """
    Processes a financial report using Gemini with a specific prompt,
    dynamically parses the result to handle formatting errors,
    and returns a clean pandas DataFrame.
    """
    print(f"Processing file: {file_path}...")
    
    prompt_name = "CAMS" if "CAMS" in prompt_to_use else "GENERAL"
    print(f"Using '{prompt_name}' prompt.")

    try:
        log.info(f"[AI Processor] Starting processing for: {os.path.basename(file_path)}")
        
        # ... (Model Selection Logic remains the same) ...
        page_count = get_pdf_page_count(file_path)
        if page_count == 0: return "ERROR: Cannot process file with 0 pages or encrypted file."
        models = ["gemini-2.5-flash", "gemini-2.5-flash-lite-preview-06-17"]
        current_idx = 0 if page_count >= 6 else 1
        model_to_use = models[current_idx]

        log.info(f"[AI Processor] Selected model '{model_to_use}' for {page_count} pages.")
        
        # ... (Gemini API Call Logic remains the same) ...
        client = genai.Client()
        myfile = client.files.upload(file=file_path)
        log.info(f"[AI Processor] File '{myfile.name}' uploaded.")
       
     # --- For testing purposes, using placeholder text ---
     # This simulates a successful AI response to avoid API calls during development.    
        try:
            response = gemini_answer(client, model_to_use,prompt_to_use,myfile)
        except Exception as e:
            log.error(e)
            if '429' or '404' in str(e):
                model_to_use = models[1 if current_idx == 0 else 0]
                response = gemini_answer(client, model_to_use,prompt_to_use,myfile) 

        client.files.delete(name=myfile.name)
        log.info(f"[AI Processor] Deleted uploaded file '{myfile.name}'.")


        print("AI response received. Parsing into DataFrame...")

        try:
            # "Happy Path": Try the simple, fast method for well-formatted CSVs.
            df = pd.read_csv(io.StringIO(response.text))
        except (pd.errors.ParserError, ValueError):
            # "Safety Net": If parsing fails, use the robust dynamic method.
            print("ParserError encountered. Falling back to dynamic parsing...")
            try:
                df = pd.read_csv(io.StringIO(response.text), header=None, names=range(25))
                df.columns = df.iloc[0].str.strip()
                df = df.iloc[1:].reset_index(drop=True)
                df.dropna(axis=1, how='all', inplace=True)
            except Exception as e:
                print(f"Dynamic parsing failed: {e}")
                return pd.DataFrame()

        print("Successfully parsed and cleaned the DataFrame.")
        return df
    except Exception as e:
        log.error(f"--- An ERROR occurred in the AI Processor: {e} ---")
        return f"ERROR: {e}"

# ==============================================================================
# 3. EXCEL REPORTING FUNCTION (WORKER FUNCTION)
# ==============================================================================

def generate_excel_report(df, output_path):

    print(f"Generating Excel report at: {output_path}")

    if df.empty:
        print("⚠️ Warning: The DataFrame is empty. No data was extracted. Aborting Excel generation.")
        return

    # --- Data Cleaning and Type Conversion ---
    numeric_cols = ['Quantity', 'Sale_Consideration', 'Selling_Expenses', 'Net_Sale_Consideration', 'Actual_Cost_of_Acquisition', 'Indexed_Cost', 'FMV_on_31012018', 'Abs_Gain_Loss', 'Holding_Days']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    for col in ['Date_of_Purchase', 'Date_of_Transfer']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

    # --- Script-Side Calculations for Validation ---
    print("Performing calculations for validation sheet...")
    df['Calculated_Gain_Loss'] = (df['Net_Sale_Consideration'] - df['Actual_Cost_of_Acquisition']).round(2)
    if pd.api.types.is_datetime64_any_dtype(df['Date_of_Transfer']) and pd.api.types.is_datetime64_any_dtype(df['Date_of_Purchase']):
        df['Calculated_Holding_Days'] = (df['Date_of_Transfer'] - df['Date_of_Purchase']).dt.days
    else:
        df['Calculated_Holding_Days'] = "Date Error"

    # --- Initialize Excel Writer with Date Formatting ---
    writer = pd.ExcelWriter(output_path, engine='openpyxl', datetime_format='DD-MM-YYYY', date_format='DD-MM-YYYY')

    # --- Write Validation Sheet (Sheet 0) ---
    print("Writing 'Validation' sheet...")
    validation_df = df[['Particulars', 'Calculated_Gain_Loss', 'Calculated_Holding_Days', 'Date_of_Purchase', 'Date_of_Transfer']].copy()
    if 'Abs_Gain_Loss' in df.columns: validation_df['Extracted_Gain_Loss'] = df['Abs_Gain_Loss']
    else: validation_df['Extracted_Gain_Loss'] = "Not Found"
    if 'Holding_Days' in df.columns: validation_df['Extracted_Holding_Days'] = df['Holding_Days']
    else: validation_df['Extracted_Holding_Days'] = "Not Found"
    final_validation_order = ['Particulars', 'Extracted_Gain_Loss', 'Calculated_Gain_Loss', 'Extracted_Holding_Days', 'Calculated_Holding_Days', 'Date_of_Purchase', 'Date_of_Transfer']
    validation_df[final_validation_order].to_excel(writer, sheet_name='Validation', index=False)

    # --- Write "Gains on STT paid shares" Sheet (Sheet 1) ---
    df_equity = df[df['Transaction_Type'] == 'EQUITY_MF'].copy()
    if not df_equity.empty:
        print("Writing 'Gains on STT paid shares' sheet...")
        df_equity['HoldingPeriod'] = df_equity['Calculated_Holding_Days']
        is_long_term = df_equity['HoldingPeriod'] > 365
        purchase_before_cutoff = df_equity['Date_of_Purchase'] < pd.to_datetime('2018-02-01')
        def calculate_deductible_cost(row):
            if not is_long_term.loc[row.name] or not purchase_before_cutoff.loc[row.name]: return row['Actual_Cost_of_Acquisition']
            lower_of_fmv_and_sale = min(row['FMV_on_31012018'], row['Net_Sale_Consideration'])
            return max(row['Actual_Cost_of_Acquisition'], lower_of_fmv_and_sale)
        df_equity['Cost of Acquisition deductible'] = df_equity.apply(calculate_deductible_cost, axis=1).round(2)
        gain_loss = (df_equity['Net_Sale_Consideration'] - df_equity['Cost of Acquisition deductible']).round(2)
        df_equity['Short term gain u/s 111A'] = df_equity.apply(lambda r: (r['Net_Sale_Consideration']-r['Actual_Cost_of_Acquisition']) if not is_long_term.loc[r.name] else 0, axis=1).round(2)
        df_equity['LTCG u/s 112A'] = gain_loss.where(is_long_term, 0)
        df_equity['Loss ignored u/s 94(7)/(8)'] = ''
        if 'ISIN_Code' not in df_equity.columns: df_equity['ISIN_Code'] = ''
        equity_output = df_equity[['Particulars', 'Quantity', 'Date_of_Purchase', 'Date_of_Transfer', 'Sale_Consideration', 'Selling_Expenses', 'Net_Sale_Consideration', 'Actual_Cost_of_Acquisition', 'Cost of Acquisition deductible', 'Short term gain u/s 111A', 'LTCG u/s 112A', 'Loss ignored u/s 94(7)/(8)', 'ISIN_Code']]
        equity_output.to_excel(writer, sheet_name='Gains on STT paid shares', index=False)

    # --- Write "Non-Equity & Debt Funds" Sheet (Sheet 2) ---
    df_non_equity_types = ['DEBT_MF_INDEXED', 'DEBT_MF_SLAB_RATE', 'OTHER_NON_EQUITY']
    df_debt = df[df['Transaction_Type'].isin(df_non_equity_types)].copy()
    if not df_debt.empty:
        print("Writing 'Non-Equity & Debt Funds' sheet...")
        df_debt['HoldingPeriod'] = df_debt['Calculated_Holding_Days']
        conditions = [df_debt['Transaction_Type'] == 'DEBT_MF_INDEXED', df_debt['Transaction_Type'] == 'DEBT_MF_SLAB_RATE', df_debt['Transaction_Type'] == 'OTHER_NON_EQUITY']
        choices = ['Debt (Indexation)', 'Debt (Slab Rate)', 'Other (Indexation)']
        df_debt['Taxation_Type'] = np.select(conditions, choices, default='Unknown')
        gain = (df_debt['Net_Sale_Consideration'] - df_debt['Actual_Cost_of_Acquisition']).round(2)
        ltcg_indexed = (df_debt['Net_Sale_Consideration'] - df_debt['Indexed_Cost']).round(2)
        st_gain_conditions = [df_debt['HoldingPeriod'] <= 1095, df_debt['Transaction_Type'] == 'DEBT_MF_SLAB_RATE']
        df_debt['Short term gain'] = np.where(np.logical_or.reduce(st_gain_conditions), gain, 0)
        ltcg_conditions = (df_debt['HoldingPeriod'] > 1095) & (df_debt['Transaction_Type'] != 'DEBT_MF_SLAB_RATE')
        df_debt['LTCG'] = np.where(ltcg_conditions, ltcg_indexed, 0)
        df_debt['Loss ignored u/s 94(7)/(8)'] = ''
        debt_output = df_debt[['Particulars', 'Quantity', 'Date_of_Purchase', 'Date_of_Transfer', 'Sale_Consideration', 'Indexed_Cost', 'Short term gain', 'LTCG', 'Loss ignored u/s 94(7)/(8)', 'Taxation_Type']]
        debt_output.to_excel(writer, sheet_name='Non-Equity & Debt Funds', index=False)

    # --- Write "Virtual Digital Assets" Sheet (Sheet 3) ---
    df_vda = df[df['Transaction_Type'] == 'VDA'].copy()
    if not df_vda.empty:
        print("Writing 'Virtual Digital Assets' sheet...")
        df_vda['Income (loss ignored)'] = (df_vda['Sale_Consideration'] - df_vda['Actual_Cost_of_Acquisition']).round(2).clip(lower=0)
        df_vda['Head of Income'] = 'Capital Gains u/s 115BBH'
        vda_output = df_vda[['Particulars', 'Date_of_Purchase', 'Date_of_Transfer', 'Sale_Consideration', 'Actual_Cost_of_Acquisition', 'Income (loss ignored)', 'Head of Income']]
        vda_output.to_excel(writer, sheet_name='Virtual Digital Assets', index=False)

    # --- Finalize and Save the Excel File ---
    writer.close()
    print(f"\n✅ Successfully generated Excel report: {output_path}")


# ==============================================================================
# 4. MAIN EXECUTION BLOCK
# ==============================================================================
    

def process_cg(input_file_path, output_file_path):
# --- Logic to choose the correct prompt based on filename ---

    log.info(input_file_path)
    print(f"{input_file_path}")

    selected_prompt = GENERAL_PROMPT  # Start with the default
    if 'cams' in input_file_path.lower():
        selected_prompt = CAMS_PROMPT
    
    # --- Execute the entire data extraction and processing pipeline ---
    master_df = process_capital_gains_report(input_file_path, selected_prompt)
    master_df.to_excel("CGR.xlsx", engine="openpyxl")
    # --- Check for valid data before generating the report ---
    if master_df is not None:
        print("\nMaster DataFrame successfully created:")
        print(master_df.head())
        print("\nDataFrame Info:")
        master_df.info()
        
        # Call the reporting function to generate the final Excel file
        generate_excel_report(master_df, output_file_path)
        return "SUCCESS"
    else:
        print("\nProcess finished, but no data was extracted. Excel report not generated.")
        return "ERROR"