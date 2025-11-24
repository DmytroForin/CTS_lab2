FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000 5001 5002 5003

CMD ["python", "coordinator.py"]
CMD ["python", "leader.py"]
CMD ["python", "follower.py"]
