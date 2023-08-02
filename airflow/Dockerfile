# First-time build can take upto 10 mins.
FROM apache/airflow:2.3.0


# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python dependencies from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt