FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kita biarkan kosong tanpa CMD, karena di docker-compose.yml 
# kita sudah set command: tail -f /dev/null agar container standby terus.