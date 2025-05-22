from dotenv import load_dotenv 
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import os
import requests
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()

# Database connection settings
DB_CONFIG = {
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME'),
}

# Encode password for secure connection
encoded_password = quote_plus(DB_CONFIG['password'])

# Create a connection to PostgreSQL
connection_uri = f"postgresql+psycopg2://{DB_CONFIG['username']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(connection_uri)

# Query data from the database
query = """
SELECT service_name, health_status, timestamp::date AS date
FROM cloudera_service_status
WHERE timestamp >= NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;
"""

df = pd.read_sql_query(query, engine)

df['date'] = df['date'].astype(str)

# Pivot data: Service Names as rows, Dates as columns, Health Status as values
pivot_df = df.pivot(index='service_name', columns='date', values='health_status').fillna("N/A")

# Define status colors
status_colors = {
    "GOOD": "#4CAF50",
    "CONCERNING": "#FF9800",
    "BAD": "#F44336",
    "N/A": "#BDBDBD"
}

# Get unique services and dates from the DataFrame
services = pivot_df.index.tolist()
dates = pivot_df.columns.tolist()

# Create a Figure and Set Dynamic Size
num_rows = len(services)
num_cols = len(dates)
fig, ax = plt.subplots(figsize=(max(8, num_cols * 1.2), max(5, num_rows * 0.6)))

# Draw Colored Blocks
for i in range(num_rows):
    for j in range(num_cols):
        value = pivot_df.iloc[i, j]
        color = status_colors.get(value, "white")
        ax.add_patch(plt.Rectangle((j, num_rows - i - 1), 1, 1, color=color, ec='black', lw=1))

# Set Axis Labels
ax.set_xticks([i + 0.5 for i in range(num_cols)])
ax.set_xticklabels(dates, fontsize=12, rotation=45, ha='right')
ax.set_yticks([i + 0.5 for i in range(num_rows)])
ax.set_yticklabels(services, fontsize=12, fontweight="bold", ha='right')

ax.set_xlim(0, num_cols)
ax.set_ylim(0, num_rows)
ax.tick_params(axis='x', bottom=False, top=True, labeltop=True, labelbottom=False)
ax.tick_params(axis='y', left=False, right=True, labelright=False, labelleft=True)

plt.title("Cloudera Service Health Status (Last 7 Days)", fontsize=14, fontweight="bold")

# Legend
legend_labels = ["GOOD", "CONCERNING", "BAD", "N/A"]
legend_colors = [status_colors["GOOD"], status_colors["CONCERNING"], status_colors["BAD"], status_colors["N/A"]]
ax.legend(
    handles=[plt.Rectangle((0, 0), 1, 1, color=color) for color in legend_colors],
    labels=legend_labels,
    loc="upper right",
    fontsize=12,
    frameon=True
)

# Save Image with Timestamp
RESULT_DIR = "/home/user/airflow/maintain/maintain_refactor/result/service_status"
os.makedirs(RESULT_DIR, exist_ok=True)
timestamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")
image_path = f"{RESULT_DIR}/service_health_{timestamp}.png"
plt.savefig(image_path, bbox_inches='tight', dpi=300)
plt.show()

# Function to send image to Mattermost
def send_mattermost_image(image_path):
    MATTERMOST_TOKEN = os.getenv("BEARER_TOKEN")
    MATTERMOST_CHANNEL_ID = os.getenv("CHANNEL_ID")

    if not MATTERMOST_TOKEN or not MATTERMOST_CHANNEL_ID:
        print("❌ Mattermost credentials missing")
        return
    
    url = "https://chat.rtarf.mi.th/api/v4/files"
    headers = {"Authorization": f"Bearer {MATTERMOST_TOKEN}"}

    with open(image_path, 'rb') as img_file:
        files = {'files': img_file}
        data = {'channel_id': MATTERMOST_CHANNEL_ID}

        response = requests.post(url, headers=headers, files=files, data=data)

    if response.status_code == 201:
        file_info = response.json()
        file_id = file_info.get("file_infos", [{}])[0].get("id")
        if file_id:
            print(f"✅ Image uploaded successfully, file_id: {file_id}")
            send_mattermost_post(file_id, MATTERMOST_CHANNEL_ID)
        else:
            print("❌ File uploaded but no file ID returned.")
    else:
        print(f"❌ Failed to upload image: {response.text}")

def send_mattermost_post(file_id, channel_id):
    MATTERMOST_TOKEN = os.getenv("BEARER_TOKEN")
    url = "https://chat.rtarf.mi.th/api/v4/posts"
    headers = {
        "Authorization": f"Bearer {MATTERMOST_TOKEN}",
        "Content-Type": "application/json"
    }

    message_data = {
        "channel_id": channel_id,
        "message": "📊 **Cloudera Service Health Status (Last 7 Days)**",
        "file_ids": [file_id]
    }

    response = requests.post(url, headers=headers, json=message_data)
    
    if response.status_code == 201:
        print("✅ Mattermost message sent successfully with the image.")
    else:
        print(f"❌ Failed to send Mattermost message: {response.text}")

# Send to Mattermost
send_mattermost_image(image_path)
