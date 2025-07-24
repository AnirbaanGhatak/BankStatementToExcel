import os
import pathlib
import shutil
import time
import json
from datetime import datetime
from dotenv import load_dotenv
from logging_config import log
import pdf_processor  # Import our refactored AI engine


# --- Configuration ---
INPUT_FOLDER = r"\\pbserver\G\BankStatementConverter\PDF_Input_(Upto_7MB_AND_No_Capital_Gains)"
PROCESSED_FOLDER = r"\\pbserver\G\BankStatementConverter\Processed_pdfs"
OUTPUT_FOLDER = r"\\pbserver\G\BankStatementConverter\Excel_Output"
REJECTED_FOLDER = r"\\pbserver\G\BankStatementConverter\Rejected_files"
IN_PROCESS_FOLDER = r"\\pbserver\G\BankStatementConverter\In_Process" # New folder for atomic moves
STATUS_FILE = r'C:\Users\DELL\coe\bstpf\processing_status.json' # Assuming code lives here

FILE_SIZE_LIMIT_BYTES = 7 * 1024 * 1024
SLEEP_INTERVAL = 15

def update_status(status_message, filename=None):
    """Writes the current status to a shared JSON file for the dashboard."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_data = {"status": status_message, "filename": filename, "last_update": now}
    try:
        with open(STATUS_FILE, 'w') as f:
            json.dump(status_data, f)
        log.info(f"Status updated: {status_message} - {filename or ''}")
    except Exception as e:
        log.error(f"Could not write to status file: {e}")

def main():
    """The main worker function that runs the continuous workflow loop."""
    log.info("--- Automated PDF Processor Worker Started ---")
    
    # Ensure all folders exist at startup
    for folder in [INPUT_FOLDER, PROCESSED_FOLDER, OUTPUT_FOLDER, REJECTED_FOLDER, IN_PROCESS_FOLDER]:
        os.makedirs(folder, exist_ok=True)
    
    update_status("Idle") # Set initial status

    while True:
        processing_path = None
        try:
            pending_files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith('.pdf')])

            if not pending_files:
                update_status("Idle")
                time.sleep(SLEEP_INTERVAL)
                continue

            filename = pending_files[0]
            source_path = os.path.join(INPUT_FOLDER, filename)
            
            # --- File Size Validation ---
            if os.path.getsize(source_path) > FILE_SIZE_LIMIT_BYTES:
                log.warning(f"REJECTED: File '{filename}' is too large.")
                rejected_path = os.path.join(REJECTED_FOLDER, f"{pathlib.Path(filename).stem}_REJECTED-TOO-LARGE.pdf")
                shutil.move(source_path, rejected_path)
                continue # Skip to next file

            # --- Atomic Move to In-Process ---
            log.info(f"\nFound new file: {filename}. Moving to process.")
            update_status("Processing", filename)
            processing_path = os.path.join(IN_PROCESS_FOLDER, filename)
            shutil.move(source_path, processing_path)

            # --- Delegate to AI Processor ---
            basename = pathlib.Path(filename).stem
            output_excel_path = os.path.join(OUTPUT_FOLDER, f"{basename}.xlsx")
            
            result = pdf_processor.process_pdf(processing_path, output_excel_path)

            # --- Handle Result ---
            if "ERROR" in result:
                log.error(f"Processing failed for '{filename}'. Moving back to input queue. Details: {result}")
                # The 'finally' block will handle moving the file back.
            else:
                log.info(f"SUCCESS: Processing complete for '{filename}'.")
                processed_pdf_path = os.path.join(PROCESSED_FOLDER, os.path.basename(processing_path))
                shutil.move(processing_path, processed_pdf_path)
                processing_path = None # Clear path so 'finally' block doesn't move it back

        except Exception as e:
            log.critical(f"--- A CRITICAL ERROR occurred in the main loop: {e} ---")
            # If a file was being processed during a critical error, the 'finally' block will handle it.
            time.sleep(60) # Wait longer after a critical failure
        
        finally:
            # This block ensures a file is never stuck in the 'In_Process' folder
            if processing_path and os.path.exists(processing_path):
                log.warning(f"Moving failed file '{os.path.basename(processing_path)}' back to input queue.")
                shutil.move(processing_path, os.path.join(INPUT_FOLDER, os.path.basename(processing_path)))
            
            update_status("Idle")

if __name__ == '__main__':
    load_dotenv()
    # You will also need a logging_config.py file for this to work
    main()