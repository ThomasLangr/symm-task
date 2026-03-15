API_URL = "https://api.fake-eshop.cz/v1"
API_KEY = "symma-secret-token"
headers = {"X-Api-Key": API_KEY,
           "Content-Type": "application/json"}
RATE_LIMIT = 5
REQUEST_DELAY = 1 / RATE_LIMIT