FROM python:3.11-slim
WORKDIR /app
COPY sources/requirements.txt /app/sources/requirements.txt
RUN pip install --no-cache-dir -r /app/sources/requirements.txt
COPY sources/ /app/sources/
ENV PYTHONPATH=/app
ENTRYPOINT ["python3"]
