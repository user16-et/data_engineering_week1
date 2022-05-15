FROM python:3.9
WORKDIR /app
COPY upload_data.py upload_data.py 
RUN pip install pandas sqlalchemy psycopg2
ENTRYPOINT [ "python","upload_data.py" ]