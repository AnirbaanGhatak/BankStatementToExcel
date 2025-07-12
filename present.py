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

load_dotenv()

INPUT_FOLDER = "Z:\\PDF_input"
OUTPUT_FOLDER = "Z:\\Excel_output"
SLEEP_INTERVAL = 15 # Seconds to wait between checking the input folder

def pdf_processor(input_path, output_path):
    try: 
        print("hi")
        client = genai.Client(api_key="AIzaSyDWbdS65v-QmV5E7jax_kV5Mq-3STC1pIU")
        print("ko")
        try: 
            myfile = client.files.upload(file=input_path)
        except Exception as e:
            print(e)
        print("hello")
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
        response = client.models.generate_content(
        model="gemini-2.5-flash-preview-04-17",
        contents=[prompt, myfile])

        client.files.delete(name=myfile.name)

        doc = io.StringIO(response.text)

        df = pd.read_csv(doc, sep=',',engine='python')
        print(df.tail(5))

        df.to_excel(output_path,index=True)

        return "Success"

    except Exception as e:
        return "ERROR"


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

            # Define the full path for the input file
            input_pdf_path = os.path.join(INPUT_FOLDER, filename)
            output_pdf_path = os.path.join(OUTPUT_FOLDER, filename)
            
            print(f"Starting Vertex AI processing for '{filename}'...")
            result_path = pdf_processor(input_pdf_path, output_pdf_path)
            
            if "ERROR" in result_path:
                # If processing fails, the original PDF is left in the input folder
                # for manual review or another attempt.
                print("Error")
                # We should probably wait a bit longer after a failure to avoid rapid retries on a bad file.
                time.sleep(30)
            else:
                os.remove(input_pdf_path)
                
                print(f"SUCCESS: Created in the output folder and deleted the original PDF.")

        except Exception as e:
            print(f"--- A CRITICAL ERROR OCCURRED IN THE MAIN LOOP: {e} ---")
            time.sleep(60)

if __name__ == '__main__':
    main()