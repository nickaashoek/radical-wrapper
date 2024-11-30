import requests
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", dest="url", type=str, help="URL to invoke")
    args = parser.parse_args()

    s = requests.session()
    request_body = {
        "id": "01235d6b-cfe3-4a5d-9885-97da1604fe8c",
        "args": {
            "target-user": "user-1"     
        } 
    }
    req_json = json.dumps(request_body)
    response = s.post(args.url, data=req_json)
    print("Response Received", response)
    try:
        print(response.json())
    except Exception as e:
        print("Error parsing response json", e)
    
    