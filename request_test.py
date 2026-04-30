import requests

url = "http://127.0.0.1:8000/api/request_resource/"
payload = {
    "names": "resource_one",
    "owner": "pplesnia",
    "session_id": "1234567890",
    "session_name": "test_session",
}

# verify=False mimics the -k flag in curl (disables SSL verification)
response = requests.post(url, data=payload, verify=False)
print(f"Status Code: {response.status_code}")
print(response.json())
