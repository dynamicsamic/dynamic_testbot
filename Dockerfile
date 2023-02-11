FROM python:3.11-slim

WORKDIR /app

COPY ./requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

COPY main.py files.py settings.py utils.py ./

COPY handlers ./handlers

COPY .env .

COPY log_config.conf .

CMD ["python", "main.py"]

