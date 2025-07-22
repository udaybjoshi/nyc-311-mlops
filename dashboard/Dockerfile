# Streamlit dashboard
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libssl-dev libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt streamlit boto3 mlflow pandas matplotlib
    

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

