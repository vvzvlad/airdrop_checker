FROM python:3.9

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY airdrop_checker.py ./
CMD while true; do python airdrop_checker.py; sleep 10; done