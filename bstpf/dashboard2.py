import streamlit as st
import os
import json
from datetime import datetime
import time
import pdf_processor
from io import BytesIO
import pandas
# --- Configuration ---

st.set_page_config(
    page_title="PDF Processor",
    page_icon="📄",
    layout="centered"
)

# processed = pandas.DataFrame()

# --- The Dashboard App ---
st.title("🤖 PDF Converter")

output = BytesIO()

@st.cache_data
def convert_to_excel(df):
    excel_buffer = BytesIO()
    with pandas.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name = "Sheet1")
    return excel_buffer.getvalue()


uploaded_file = st.file_uploader(":red-badge[⚠️ ONLY FILES WITHOUT PASSWORDS]", accept_multiple_files=False, key=None, help=None, args=None, kwargs=None, disabled=False, label_visibility="visible", width="stretch")

if uploaded_file is not None:

    with st.spinner("Wait for it...", show_time=True):
        
        #also set difference and a option box if its manual and not ocr
        #then manual code else AI ocr
        
        
        processed = pdf_processor.process_pdf(uploaded_file)

    if isinstance(processed, pandas.DataFrame):
        if not processed.empty:
            st.success("PDF processed Successfully")
            st.dataframe(processed)
            
            excel_data = convert_to_excel(processed)

            st.download_button("Converted Excel to download",data= excel_data, file_name=None, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=None, help=None, disabled=False)
        
        
        else:
            st.warning("Processing complete, but no data was extracted from the PDF.")
        
    else:
        st.error("Failed to Process PDF")
        
        if processed:
            st.code(f"Processor Message: {processed}")
            