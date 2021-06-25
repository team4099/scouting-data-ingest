FROM python:3.8-slim
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
