from google import genai
from google.genai import types
import pathlib
import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

client = genai.Client()

# Retrieve and encode the PDF byte
filepath = pathlib.Path('Z:\\Maitra & Co\\Clients\\S\\Sodha Ashwin_Heena\\Ashwin Sodha\\AY 2025-26\\Income Tax\\Data from client\\Ashwin Sodha- 1_merged.pdf')

prompt = """Your task is to act as a meticulous data extraction assistant. Analyze the provided bank statement PDF and extract all transaction data into a clean, raw CSV format.
Follow these instructions precisely:
1. CSV Headers: Use the following exact headers for the CSV output:
TransactionDate,Narration,ChequeRefNo,ValueDate,WithdrawalAmount,DepositAmount,ClosingBalance
2. Data Extraction Rules:
The "Narration" column in the PDF often spans multiple lines for a single transaction. You must combine all lines of the narration for a single transaction into one field in the CSV. Use a single space to separate the combined text.
The first entry is "BALANCE BROUGHT FORWARD". Treat this as the first data row.
Process all pages of the document and create a single, continuous CSV. Ignore the repeating table headers on all subsequent pages.
3. Data Cleaning Rules:
For the WithdrawalAmount, DepositAmount, and ClosingBalance columns, remove all commas (e.g., "305,951.68" must become "305951.68").
If a withdrawal or deposit amount is empty or "0.00", represent it as 0.00.
4. Output Format:
Your entire response must only be the raw CSV text.
Do not include any introductory sentences, explanations, summaries, or markdown code blocks like csv. Start directly with the header row."""

response = client.models.generate_content(
  model="gemini-2.5-flash",
  contents=[
      types.Part.from_bytes(
        data=filepath.read_bytes(),
        mime_type='application/pdf',
      ),
      prompt])

# df = pandas.

print(response.text)

def pdfconvert():

  df = pd.DataFrame(response.candidates.)
  print(df.head(-1))
