FROM python:3.10-slim-buster

# Set the working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install jq bash curl libpq-dev libpq5 python3-dev build-essential -y && \
    python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt


# install cli tools
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN apt-get update && apt-get install google-cloud-cli -y

# Expose the application port
EXPOSE 5000

# Run the application
CMD [ "python3", "app.py"]
