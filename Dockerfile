# Usually start from the with FROM --> What kind of Base image we want to use
FROM python:3.10.13

RUN pip install pandas sqlalchemy psycopg2

# From current directory to the docker image
# You can also specify the working directory: The location in the image in the container where the file is copied to
# it will Create this directory and do cd to this directory
WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]