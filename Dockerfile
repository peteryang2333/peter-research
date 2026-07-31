FROM python:3.11-slim

# multi-arch (works on Oracle ARM a1.flex and amd64 e2.micro)
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_BROWSER_SERVERADDRESS=0.0.0.0 \
    JOURNAL_DB=/data/journal.db

# run as non-root
RUN useradd -m appuser && mkdir -p /data && chown -R appuser /data /app
USER appuser

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
