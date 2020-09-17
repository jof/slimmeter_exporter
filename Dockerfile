FROM python:3.8-alpine
RUN pip install dsmr-parser prometheus-client circuits
RUN pip install ptvsd
COPY exporter.py /usr/local/bin/exporter.py
CMD ["python3", "/usr/local/bin/exporter.py"]
