FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["python", "-c", "print('ML runner is ready. Add the project training/scoring entrypoint when the feature contract is finalized.')"]
