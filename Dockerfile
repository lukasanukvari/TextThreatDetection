FROM bitnami/spark:3.4.1

USER root

# Install pip and Python packages
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy entire project
COPY . /app
WORKDIR /app
