# AWS Glue OCR Extraction

This repository contains AWS Glue jobs for extracting text from PDFs stored in an S3 bucket. The job processes images within PDFs using `pdf2image` and `pytesseract` to perform OCR (Optical Character Recognition) and stores the extracted text back in S3.

## Project Overview
### Files & Scripts

#### 1. `text_extraction_ocr.py`
- Fetches PDF files from an S3 bucket.
- Converts PDF pages to images.
- Extracts text from images using `pytesseract`.
- Stores the extracted text in a single file and uploads it to S3.

**Run locally:**
```sh
pip install pytesseract pdf2image
python3 text_extraction_ocr.py
```

#### 2. `glue_ocr_extraction.py`
- Similar to `text_extraction_ocr.py` but designed for AWS Glue.
- Uses Spark and `boto3` to process files and push extracted text to S3.
- Uses `GlueContext` to manage data processing.

**Run locally:**
- Build and run the project in Docker (instructions below).

#### 3. `glue_ocr_extraction.py`
- Similar to the above but specifically utilizes `GlueContext` to create an S3 context.
- Creates a Glue job to process PDFs and extract text.
- To run locally, build the Docker image and execute it as described below.

## Prerequisites
Ensure the following dependencies are installed:
- Docker
- AWS credentials (Access Key, Secret Key, and Session Token)
- Workspace environment variable
- Pull the repo --> https://github.com/vipuld-ht/Text-extraction-ocr.git
## Setup & Execution

### 1. Export Workspace Location
```sh
# redirect to workspace 
pwd
export WORKSPACE_LOCATION="your_workspace_location from pwd"
```

### 2. Build the Docker Image
```sh
docker build -t glue-ocr-runner .
```

### 3. Run the Docker Container
```sh
docker run -it --rm \
    --name glue_jupyter_lab \
    -v "${WORKSPACE_LOCATION:?Workspace location not set}:/home/glue_user/workspace/jupyter_workspace" \
    -e DISABLE_SSL=true \
    -p 4040:4040 \
    -p 18080:18080 \
    -p 8998:8998 \
    -p 8888:8888 \
    glue-ocr-runner \
    /home/glue_user/jupyter/jupyter_start.sh
```

### 4. Run it using dev container or Jupyter Lab
#### -  Connect to dev container from vscode or terminal 
#### -  Connect to Jupyter Lab
- Open [http://localhost:8888](http://localhost:8888) in your browser.
- This will allow you to manage and execute your Glue job efficiently.

### 5. Configure AWS Credentials in Container
Once inside the container or Jupyter Lab, configure AWS credentials:
```sh
export AWS_REGION="your_account_region"
export AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="YOUR_AWS_SESSION_TOKEN"
```

## Notes
- Make sure the AWS credentials provided have the necessary permissions for accessing S3 and running Glue jobs.
- Modify `glue_ocr_extraction.py` as needed for specific configurations or improvements.

## License
This project is open-source and can be modified as per requirements.