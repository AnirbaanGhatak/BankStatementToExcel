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

if 'uploaded_file_details' not in st.session_state:
    st.session_state.uploaded_file_details = None
if 'processing_status' not in st.session_state:
    st.session_state.processing_status = "idle"
if 'processed_df' not in st.session_state:
    st.session_state.processed_df = None
if 'processor_message' not in st.session_state:
    st.session_state.processor_message = None
    
def convert_to_excel(df):
    excel_buffer = BytesIO()
    with pandas.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name = "Sheet1")
    return excel_buffer.getvalue()


uploaded_file = st.file_uploader(":red-badge[⚠️ ONLY FILES WITHOUT PASSWORDS]", 
                                 accept_multiple_files=False, 
                                 key="file_uploader_widget", 
                                 help=None, 
                                 args=None, 
                                 kwargs=None, 
                                 disabled=False, 
                                 label_visibility="visible", 
                                 width="stretch")

if uploaded_file is not None:
    
    if st.session_state.uploaded_file_details is None or uploaded_file.name != st.session_state.uploaded_file_details['name']:
            st.session_state.uploaded_file_details = {
                'name':uploaded_file.name,
                'content': uploaded_file.getvalue()
            }
            
            st.session_state.processed_df = None
            st.session_state.processing_status = "processing"
            st.session_state.processor_message = None
            
if st.session_state.processing_status == "processing" and st.session_state.uploaded_file_details:
    file_details = st.session_state.uploaded_file_details
    
    file_for_processor = BytesIO(file_details['content'])
    file_for_processor.name = file_details['name']
    
    
    with st.spinner("Wait for it...", show_time=True):
        
        #also set difference and a option box if its manual and not ocr
        #then manual code else AI ocr
        
        try:    
            processed = pdf_processor.process_pdf(uploaded_file)
        
            st.session_state.processed_df = processed
    
            if isinstance(processed, pandas.DataFrame):
                if not processed.empty:
                    st.session_state.processing_status = "completed"
                    st.session_state.processor_message = "PDF processed Successfully"
                    
                else:
                    st.session_state.processing_status = "failed"
                    st.session_state.processor_message = "Processing complete but no Data available"
                    st.success("PDF processed Successfully")
                    
            else:
                st.session_state.processing_status = "failed"
                st.session_state.processing_message = f"**Processing Failed.** The PDF may be password-protected or corrupted. Processor Message: {processed}"
                    
                    
        except Exception as e:
            st.session_state.processing_status = "failed"
            st.session_state.processor_message = "An unexpected error occurred during processing: {e}"
            st.error(f"Unexpected Error Occurred: {e}")    
                
                
                
if st.session_state.uploaded_file_details is None:
    st.info("Please upload a BankStatement PDF under 7MB to begin Processing")
    
elif st.session_state.processing_status == "processing":
    st.info(f"Processing {st.session_state.uploaded_file_details['name']}...")
    
elif st.session_state.processing_status == "completed":
    st.success(st.session_state.processor_message)
    st.dataframe(st.session_state.processed_df)
    
    excel_data = convert_to_excel(st.session_state.processed_df)
    
    foil_name = f"{os.path.splitext(st.session_state.uploaded_file_details['name'])[0]}_converted.xlsx"

    st.download_button("Converted Excel to download",
                               data= excel_data, 
                               file_name=foil_name, 
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                               key="download_button_widget", 
                               help=None, 
                               disabled=False)
elif st.session_state.processing_status == "failed":
    st.error(st.session_state.processor_message)
    
            