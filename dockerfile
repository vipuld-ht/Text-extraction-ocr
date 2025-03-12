FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

# Enable EPEL repo and install system dependencies
RUN amazon-linux-extras enable epel && \
    yum install -y tesseract poppler-utils python3-pip && \
    yum clean all

# Ensure pip is available
RUN python3 -m ensurepip && \
    python3 -m pip install --upgrade pip

# Switch to glue_user
USER glue_user

# Create a working directory and copy requirements.txt
WORKDIR /home/glue_user
COPY requirements.txt .

# Install Python dependencies using pip3
RUN python3 -m pip install --no-cache-dir -r requirements.txt

CMD ["/home/glue_user/jupyter/jupyter_start.sh"]
