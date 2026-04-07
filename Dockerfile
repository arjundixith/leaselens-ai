FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
ENV GOOGLE_CLOUD_PROJECT=learn-mcp-490919
ENV GOOGLE_CLOUD_LOCATION=global
ENV GOOGLE_GENAI_USE_VERTEXAI=true

CMD ["uvicorn", "lease_lens_app.main:app", "--host", "0.0.0.0", "--port", "8080"]
