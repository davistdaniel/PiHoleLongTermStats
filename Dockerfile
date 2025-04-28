FROM python:3.10-slim-bookworm

# Change the working directory to the `app` directory
WORKDIR /app

# Copy only the files needed for dependency installation
COPY requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY app.py .
COPY assets/style.css ./assets/

# Run the app
CMD ["python", "app.py"]