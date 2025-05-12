# httpay
Our standing service for facilitating payments in different currencies

# Testing
```
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
docker-compose up -d
python -m pytest -v
docker-compose down -v
```