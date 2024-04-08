FROM apache/spark:python3

COPY . .

CMD ["python", "main.py"]
