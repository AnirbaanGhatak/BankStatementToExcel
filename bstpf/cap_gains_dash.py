import streamlit as st
import os
import json
from datetime import datetime
import time

# --- Configuration ---
STATUS_FILE = r'C:\Users\DELL\coe\bstpf\processing_status_cg.json'

st.set_page_config(
    page_title="Capital Gains Converter",
    page_icon="📈",
    layout="centered"
)

# --- The Dashboard App ---
st.title("🤖 Capital Gains Converter")
st.caption(f"This dashboard automatically refreshes. Last check: {datetime.now().strftime('%H:%M:%S')}")

status_placeholder = st.empty()

uploaded_file = st.file_uploader("**Without Password Files**", type="pdf", accept_multiple_files=False, key=None, help=None, on_change=None, args=None, kwargs=None, disabled=False, label_visibility="visible", width="stretch")

if uploaded_file is not None:
        processing_file = uploaded_file.



def display_status():
    """Reads the status file and updates the Streamlit elements."""
    default_status = {"status": "Initializing...", "filename": "Waiting for worker...", "last_update": "N/A"}
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, 'r') as f:
                status_data = json.load(f)
            print("reak")
        else:
            status_data = default_status
            print("exp1")
    except Exception:
        status_data = default_status
        status_data['status'] = 'Reading Status...'

    with status_placeholder.container():
        status_message = status_data.get("status", "Unknown")
        filename = status_data.get("filename", "")

        print("inside 1")

        if status_message == "Processing":
            st.info(f"**Status:** {status_message}", icon="⏳")
            print("inside 2")
            if filename:
                st.code(f"Current File: {filename}", language=None)
                print("inside 3")
    
        else:
            st.success(f"**Status:** {status_message}", icon="✅")
            st.write("The system is ready for new files in the input folder.")

        st.write(f"_*Last worker update: {status_data.get('last_update', 'N/A')}*_")

while True:
    display_status()
    time.sleep(5)