FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

COPY app ./app

COPY .env log_config.conf main.py ./

CMD ["python", "main.py"]

