import streamlit as st
import os
import json
from datetime import datetime
import time
import pdf_processor

# --- Configuration ---
STATUS_FILE = r'C:\Users\DELL\coe\bstpf\processing_status.json'

st.set_page_config(
    page_title="PDF Processor",
    page_icon="📄",
    layout="centered"
)

# --- The Dashboard App ---
st.title("🤖 PDF Converter")
st.caption(f"This dashboard automatically refreshes. Last check: {datetime.now().strftime('%H:%M:%S')}")

status_placeholder = st.empty()


uploaded_file = st.file_uploader(":red-badge[⚠️ ONLY FILES WITHOUT PASSWORDS]", type="pdf", accept_multiple_files=False, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible", width="stretch")

if uploaded_file is not None:
        #do a file encryption check
        processing_file = uploaded_file
        print(f"file uploaded {processing_file.name}")

        pdf_processor.process_pdf()

        #https://ai.google.dev/gemini-api/docs/document-processing

        #also set difference and a option box if its manual and not ocr
        #then manual code else AI ocr




def display_status():
    """Reads the status file and updates the Streamlit elements."""
    default_status = {"status": "Initializing...", "filename": "Waiting for worker...", "last_update": "N/A"}
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                status_data = json.load(f)
        else:
            status_data = default_status
    except Exception:
        status_data = default_status
        status_data['status'] = 'Reading Status...'

    with status_placeholder.container():
        status_message = status_data.get("status", "Unknown")
        filename = status_data.get("filename", "")


        if status_message == "Processing":
            st.info(f"**Status:** {status_message}", icon="⏳")
            if filename:
                st.code(f"Current File: {filename}", language=None)
    
        else:
            st.success(f"**Status:** {status_message}", icon="✅")
            st.write("The system is ready for new files in the input folder.")

        st.write(f"_*Last worker update: {status_data.get('last_update', 'N/A')}*_")

while True:
    display_status()
    time.sleep(5)