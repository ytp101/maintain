import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
import urllib3

# Suppress InsecureRequestWarning (use with caution)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CLOUDERA_URL = 'https://10.104.4.19:7183/api/v31/clusters/RTARF_CDP/services'
CLOUDERA_AUTH = ('admin', 'Brl6TuL4vX')
LINE_NOTIFY_TOKEN = 'DbeEYA3RPe5sfnsXnN1OcyZhxjBJRCperJIldgGmQ7G'
# Acress token ตัวจริง DbeEYA3RPe5sfnsXnN1OcyZhxjBJRCperJIldgGmQ7G 
# Acress token ตัวเทส paKdQdDYxHgNhyDyEFfskvsaECJiFgiZY0NKaFUmxg6

EMOJI_MAP = {
    "GOOD": "✅",
    "CONCERNING": "⚠️",
    "BAD": "❌",
    "UNKNOWN": "❓"
}

def fetch_service_status():
    try:
        response = requests.get(CLOUDERA_URL, auth=CLOUDERA_AUTH, verify=False)
        response.raise_for_status()
    except ConnectionError:
        return "Connection failed. Please check your network."
    except Timeout:
        return "Request timed out. Please try again later."
    except RequestException as e:
        return f"An error occurred: {e}"
    
    services = response.json().get('items', [])
    if not services:
        return "No services found."
    
    message = "หมายเหตุ เครื่องหมาย ✅ ปกติ | ⚠️ เฝ้าระวัง | ❌ ขัดข้อง | ❓ ตรวจสอบไม่ได้\n"
    
    for service in services:
        name = service['displayName']
        health_summary = service['healthSummary']
        emoji = EMOJI_MAP.get(health_summary, "❓")
        message += f"{emoji} {name}: {health_summary}\n"
    
    return message

def send_line_notify(message):
    headers = {'Authorization': f'Bearer {LINE_NOTIFY_TOKEN}'}
    data = {'message': message}
    
    try:
        response = requests.post('https://notify-api.line.me/api/notify', headers=headers, data=data)
        response.raise_for_status()
    except ConnectionError:
        print("Failed to connect to Line Notify API. Please check your internet connection.")
    except Timeout:
        print("The request to Line Notify timed out. Please try again later.")
    except RequestException as e:
        print(f"Failed to send notification: {e}")

def main():
    status_message = fetch_service_status()
    send_line_notify(status_message)

if __name__ == "__main__":
    main()
