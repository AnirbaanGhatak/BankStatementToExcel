import os
from google.cloud import aiplatform

# --- Configuration ---
# TODO: Change these to your project's values
PROJECT_ID = "banktopdf"
REGION = "global"

def check_service_account_setup():
    """
    Verifies that the environment is set up to use a service account key.
    """
    print("--- Verifying Service Account Setup ---")

    # Check if the environment variable is set
    key_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not key_path:
        print("[FAILURE] The 'GOOGLE_APPLICATION_CREDENTIALS' environment variable is NOT set.")
        print("Please complete Step 4 of the guide and restart your terminal/IDE.")
        return False

    print(f"Found credentials file at: {key_path}")

    # Check if the file actually exists
    if not os.path.exists(key_path):
        print(f"\n[FAILURE] The file specified in the environment variable does not exist.")
        print("Please check that the path is correct and there are no typos.")
        return False

    try:
        # This one line will test everything using the service account
        aiplatform.init(project=PROJECT_ID, location=REGION)
        print(f"\n[SUCCESS] Successfully initialized Vertex AI using the service account.")
        print("Your local environment is correctly configured to act as the server.")
        return True

    except Exception as e:
        print(f"\n[FAILURE] An error occurred during initialization.")
        print(f"Error details: {e}")
        print("\n--- Troubleshooting ---")
        print("1. Does the service account have the 'Vertex AI User' and 'Storage Object Admin' roles?")
        print("2. Is the Vertex AI API enabled in your GCP project?")
        print("3. Did you restart your terminal/IDE after setting the environment variable?")
        return False

if __name__ == "__main__":
    check_service_account_setup()