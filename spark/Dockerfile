FROM bitnamilegacy/spark:3.5.1
USER root
WORKDIR /opt/spark_jobs
COPY spark/requirements.txt /opt/spark_jobs/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark_jobs/requirements.txt
COPY spark/streaming_job.py /opt/spark_jobs/streaming_job.py
COPY spark/batch_job.py /opt/spark_jobs/batch_job.py
