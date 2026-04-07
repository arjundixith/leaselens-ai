FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
ENV LEASELENS_BACKEND_URL=https://lease-lens-ai-6093695901.us-central1.run.app

CMD ["uvicorn", "webapp:app", "--host", "0.0.0.0", "--port", "8080"]
