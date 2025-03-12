import os
import boto3
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

# AWS Configuration

BUCKET_NAME = "insightrag-job-config"
S3_PDF_FOLDER = "input/"  # Folder in S3 where PDFs are stored
S3_OUTPUT_FOLDER = "output-glue/"  # Folder in S3 where text files will be uploaded
LOCAL_DOWNLOAD_FOLDER = "downloaded_pdfs"  # Local folder for downloaded PDFs
LOCAL_OUTPUT_FOLDER = "extracted_text"  # Local folder for extracted text

# Set the path to Tesseract executable if not in PATH
# Example for Windows: pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Initialize S3 client
s3 = boto3.client("s3")

def download_pdfs_from_s3():
    """Downloads all PDFs from the specified S3 folder."""
    if not os.path.exists(LOCAL_DOWNLOAD_FOLDER):
        os.makedirs(LOCAL_DOWNLOAD_FOLDER)

    pdf_files = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PDF_FOLDER)
    
    if "Contents" in pdf_files:
        for obj in pdf_files["Contents"]:
            file_key = obj["Key"]
            if file_key.lower().endswith(".pdf"):
                local_path = os.path.join(LOCAL_DOWNLOAD_FOLDER, os.path.basename(file_key))
                s3.download_file(BUCKET_NAME, file_key, local_path)
                print(f"Downloaded: {file_key} -> {local_path}")
    else:
        print("No PDFs found in the specified S3 folder.")

def extract_text_from_pdf(pdf_path):
    """Extracts text from a PDF using OCR."""
    images = convert_from_path(pdf_path)  # Convert PDF to images
    text = ""
    for image in images:
        text += pytesseract.image_to_string(image) + "\n"
    return text

def process_pdfs():
    """Processes downloaded PDFs and extracts text."""
    extracted_texts = {}
    for filename in os.listdir(LOCAL_DOWNLOAD_FOLDER):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(LOCAL_DOWNLOAD_FOLDER, filename)
            extracted_texts[filename] = extract_text_from_pdf(pdf_path)
            print(f"Processed: {filename}")
    
    return extracted_texts

def save_extracted_text(extracted_data):
    """Saves extracted text to .txt files."""
    if not os.path.exists(LOCAL_OUTPUT_FOLDER):
        os.makedirs(LOCAL_OUTPUT_FOLDER)
    
    for pdf, text in extracted_data.items():
        txt_filename = f"{os.path.splitext(pdf)[0]}.txt"
        txt_path = os.path.join(LOCAL_OUTPUT_FOLDER, txt_filename)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved: {txt_path}")

def upload_txt_to_s3():
    """Uploads extracted text files to S3."""
    for filename in os.listdir(LOCAL_OUTPUT_FOLDER):
        if filename.endswith(".txt"):
            local_path = os.path.join(LOCAL_OUTPUT_FOLDER, filename)
            s3_key = os.path.join(S3_OUTPUT_FOLDER, filename)
            s3.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"Uploaded: {local_path} -> s3://{BUCKET_NAME}/{s3_key}")

if __name__ == "__main__":
    # Step 1: Download PDFs from S3
    download_pdfs_from_s3()
    
    # Step 2: Extract text using OCR
    extracted_data = process_pdfs()
    
    # Step 3: Save extracted text locally
    save_extracted_text(extracted_data)

    # Step 4: Upload extracted text back to S3
    upload_txt_to_s3()
