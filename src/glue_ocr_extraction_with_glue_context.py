import sys
import boto3
import pytesseract
from io import BytesIO
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pdf2image import convert_from_bytes
from awsglue.dynamicframe import DynamicFrame

# Initialize AWS Glue context
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
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
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
        return response["Body"].read()
    except Exception as e:
        print(f"❌ Error reading {file_key} from S3: {str(e)}")
        return None

def extract_text_from_pdf(pdf_bytes):
    """Extract text from PDF using OCR."""
    try:
        images = convert_from_bytes(pdf_bytes)  # Convert PDF bytes to images
        text = "\n".join(pytesseract.image_to_string(image) for image in images)
        return text
    except Exception as e:
        print(f"❌ OCR extraction failed: {str(e)}")
        return None

def upload_text_to_s3(file_name, text):
    """Upload extracted text to S3 using GlueContext."""
    try:
        s3_key = f"{S3_OUTPUT_FOLDER}{file_name}"
        df = spark.createDataFrame([(text,)], ["extracted_text"])
        
        # Convert DataFrame to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "Converted_text")

        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={"path": f"s3://{BUCKET_NAME}/{s3_key}"},
            format="pdf"
        )
        print(f"✅ Uploaded: s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload {file_name} to S3: {str(e)}")
        print(f"❌ Failed to upload {file_name} to S3: {str(e)}")

def process_pdfs():
    """Extracts text from PDFs and uploads results to S3."""
    pdf_files = list_pdfs_from_s3()
    if not pdf_files:
        print("⚠️ No PDF files found in S3.")
        return

    for file_key in pdf_files:
        print(f"📄 Processing: {file_key} ...")
        pdf_bytes = read_pdf_from_s3(file_key)
        if not pdf_bytes:
            continue

        extracted_text = extract_text_from_pdf(pdf_bytes)
        if not extracted_text:
            continue

        text_file_name = file_key.split("/")[-1].replace(".pdf", ".json")
        upload_text_to_s3(text_file_name, extracted_text)

# Start ETL process
process_pdfs()
print("✅ AWS Glue job completed successfully.")

# Commit job
job.commit()
