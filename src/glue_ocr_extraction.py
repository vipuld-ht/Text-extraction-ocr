import sys
import boto3
import pytesseract
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pdf2image import convert_from_bytes

# Parse job arguments
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue context
import sys
import boto3
import pytesseract
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pdf2image import convert_from_bytes

# Parse job arguments
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("ocr_text_extraction")

# AWS S3 Configuration
BUCKET_NAME = "insightrag-job-config"
S3_PDF_FOLDER = "input/"
S3_OUTPUT_FOLDER = "output_glue/"

# Initialize S3 client
s3 = boto3.client("s3")

def list_pdfs_from_s3():
    """List all PDF files from S3 input folder."""
    pdf_files = []
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PDF_FOLDER)
    
    if "Contents" in objects:
        for obj in objects["Contents"]:
            file_key = obj["Key"]
            if file_key.lower().endswith(".pdf"):
                pdf_files.append(file_key)
    return pdf_files

def read_pdf_from_s3(file_key):
    """Read a PDF file directly from S3 into memory."""
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    pdf_data = response["Body"].read()
    return pdf_data

def extract_text_from_pdf(pdf_bytes):
    """Extract text from PDF using OCR."""
    images = convert_from_bytes(pdf_bytes)  # Convert PDF bytes to images
    text = ""
    for image in images:
        text += pytesseract.image_to_string(image) + "\n"
    return text

def upload_text_to_s3(file_name, text):
    """Upload extracted text to S3."""
    s3_key = f"{S3_OUTPUT_FOLDER}{file_name}"
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=text.encode("utf-8"))
    print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")

def process_pdfs():
    """Extracts text from PDFs and uploads results to S3."""
    pdf_files = list_pdfs_from_s3()
    if not pdf_files:
        print("No PDF files found in S3.")
        return

    for file_key in pdf_files:
        print(f"Processing {file_key}...")
        pdf_bytes = read_pdf_from_s3(file_key)
        extracted_text = extract_text_from_pdf(pdf_bytes)

        text_file_name = file_key.split("/")[-1].replace(".pdf", ".txt")
        upload_text_to_s3(text_file_name, extracted_text)

# Start ETL process
process_pdfs()
print("✅ AWS Glue job completed successfully.")

# Commit job
job.commit()
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("ocr_text_extraction")

# AWS S3 Configuration
BUCKET_NAME = "insightrag-job-config"
S3_PDF_FOLDER = "input/"
S3_OUTPUT_FOLDER = "output_glue/"

# Initialize S3 client
s3 = boto3.client("s3")

def list_pdfs_from_s3():
    """List all PDF files from S3 input folder."""
    pdf_files = []
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_PDF_FOLDER)
    
    if "Contents" in objects:
        for obj in objects["Contents"]:
            file_key = obj["Key"]
            if file_key.lower().endswith(".pdf"):
                pdf_files.append(file_key)
    return pdf_files

def read_pdf_from_s3(file_key):
    """Read a PDF file directly from S3 into memory."""
    response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
    pdf_data = response["Body"].read()
    return pdf_data

def extract_text_from_pdf(pdf_bytes):
    """Extract text from PDF using OCR."""
    images = convert_from_bytes(pdf_bytes)  # Convert PDF bytes to images
    text = ""
    for image in images:
        text += pytesseract.image_to_string(image) + "\n"
    return text

def upload_text_to_s3(file_name, text):
    """Upload extracted text to S3."""
    s3_key = f"{S3_OUTPUT_FOLDER}{file_name}"
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=text.encode("utf-8"))
    print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")

def process_pdfs():
    """Extracts text from PDFs and uploads results to S3."""
    pdf_files = list_pdfs_from_s3()
    if not pdf_files:
        print("No PDF files found in S3.")
        return

    for file_key in pdf_files:
        print(f"Processing {file_key}...")
        pdf_bytes = read_pdf_from_s3(file_key)
        extracted_text = extract_text_from_pdf(pdf_bytes)

        text_file_name = file_key.split("/")[-1].replace(".pdf", ".txt")
        upload_text_to_s3(text_file_name, extracted_text)

# Start ETL process
process_pdfs()
print("✅ AWS Glue job completed successfully.")

# Commit job
job.commit()
