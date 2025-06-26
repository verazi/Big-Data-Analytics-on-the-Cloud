# COMP90024 Team 75 
# Linying Wei (1638206)
# Wangyang Wu (1641248)
# Ziyu Wang (1560831)
# Roger Zhang (1079986)


# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY task_producer.py .

# Create directory for config files 
RUN mkdir -p /configs/default/shared-data-reddit

# Set Python to run unbuffered to see logs in real-time
ENV PYTHONUNBUFFERED=1

# Use ENTRYPOINT for better container behavior
ENTRYPOINT ["python", "/app/task_producer.py"]