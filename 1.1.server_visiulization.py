import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import matplotlib.pyplot as plt
from dotenv import load_dotenv  
from datetime import datetime

# Load environment variables
load_dotenv()

MATTERMOST_TOKEN = os.getenv("BEARER_TOKEN")
MATTERMOST_CHANNEL_ID = "389wx7ehk38ajc46hex5ajndxe"

# Define the correct order for servers
SERVER_ORDER = [
    "BI Server", "Talend Server 1", "Talend Server 2", "Scheduler Server",
    "Repo Server", "Datanode 1", "Datanode 2", "Datanode 3",
    "Gatewaynode", "Activenode", "Standbynode", "Backup"
]

RESULT_DIR = "./result/server_visualization"
os.makedirs(RESULT_DIR, exist_ok=True)

def send_mattermost_notification(message, image_path, token, channel_id):
    """Send a notification to Mattermost with the image."""
    if not token or not channel_id:
        print("❌ Mattermost token or channel ID is missing. Skipping notification.")
        return None

    url = 'https://chat.rtarf.mi.th/api/v4/posts'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    # Upload the image to Mattermost
    file_url = 'https://chat.rtarf.mi.th/api/v4/files'
    
    with open(image_path, 'rb') as image_file:
        files = {'files': (image_path, image_file, 'image/png')}
        file_data = {'channel_id': channel_id}
        
        file_response = requests.post(file_url, headers={'Authorization': f'Bearer {token}'}, files=files, data=file_data)

    if file_response.status_code != 201:
        print(f"❌ Failed to upload image: {file_response.status_code}, {file_response.text}")
        return file_response

    file_info = file_response.json()
    file_id = file_info.get('file_infos', [{}])[0].get('id')

    if not file_id:
        print("❌ Failed to extract file ID from Mattermost response.")
        return None

    print(f"✅ Image uploaded successfully with file_id: {file_id}")

    # Create the post with the image attached
    post_data = {
        "channel_id": channel_id,
        "message": message,
        "file_ids": [file_id]  # Attach uploaded file
    }

    response = requests.post(url, headers=headers, json=post_data)

    if response.status_code == 201:
        print(f"✅ Mattermost notification sent for {image_path}")
    else:
        print(f"❌ Failed to send Mattermost message: {response.status_code}, {response.text}")

    return response

# Database connection details
db_config = {
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME')
}

# Encode password for connection string
encoded_password = quote_plus(db_config['password'])

# Create SQLAlchemy connection URI
connection_uri = f"postgresql+psycopg2://{db_config['user']}:{encoded_password}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(connection_uri)

# Function to fetch data using SQLAlchemy
def fetch_server_metrics():
    query = """
    SELECT server_name, datetime_record, cpu_usage_percent, used_ram_gb, total_ram_gb, used_disk_gb 
    FROM server_metrics 
    WHERE datetime_record >= NOW() - INTERVAL '7 days'
    ORDER BY server_name, datetime_record;
    """
    
    try:
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Fetch data
df = fetch_server_metrics()

# Process data if available
if df is not None and not df.empty:
    df["datetime_record"] = pd.to_datetime(df["datetime_record"])

    # Sort the data based on the predefined server order
    df["server_name"] = pd.Categorical(df["server_name"], categories=SERVER_ORDER, ordered=True)
    df = df.sort_values(by=["server_name", "datetime_record"])

    # Generate plots for each server in the correct order
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Format: YYYY-MM-DD_HH-MM-SS

    for server in SERVER_ORDER:
        server_data = df[df["server_name"] == server]

        if server_data.empty:
            print(f"⚠️ No data found for {server}, skipping...")
            continue

        fig, axes = plt.subplots(1, 3, figsize=(18, 5))

        # CPU Usage Line Plot
        axes[0].plot(server_data["datetime_record"], server_data["cpu_usage_percent"], marker='o', linestyle='-', color='blue')
        axes[0].set_title(f"{server} - CPU Usage (%)")
        axes[0].set_xlabel("Date")
        axes[0].set_ylabel("CPU Usage (%)")
        axes[0].grid(True)

        # RAM Usage Bar Plot
        max_ram = server_data["total_ram_gb"].iloc[-1]
        used_ram = server_data["used_ram_gb"].iloc[-1]
        axes[1].bar(server, max_ram, color='gray', label='Max RAM')
        axes[1].bar(server, used_ram, color='blue', label='Used RAM')
        axes[1].set_title(f"{server} - RAM Usage")
        axes[1].set_ylabel("RAM (GB)")
        axes[1].legend()

        # Disk Usage Line Plot
        axes[2].plot(server_data["datetime_record"], server_data["used_disk_gb"], marker='s', linestyle='-', color='red')
        axes[2].set_title(f"{server} - Disk Usage (GB)")
        axes[2].set_xlabel("Date")
        axes[2].set_ylabel("Disk Usage (GB)")
        axes[2].grid(True)

        plt.tight_layout()

        # Save plot with timestamp
        image_path = f"{RESULT_DIR}/{server}_{timestamp}.png"
        plt.savefig(image_path)
        plt.close()

        print(f"✅ Plot saved for {server}: {image_path}")

        # Send notification to Mattermost
        message = f"📊 **Server Resource Usage Report - {server}**\n🕒 {timestamp}"
        send_mattermost_notification(message, image_path, MATTERMOST_TOKEN, MATTERMOST_CHANNEL_ID)

    print(f"✅ All plots saved and sent to Mattermost.")

else:
    print("No data retrieved from the database.")
