import os
import pandas as pd
import requests
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv  
from datetime import datetime

# Load environment variables
load_dotenv()

# Mattermost Configuration
MATTERMOST_TOKEN = os.getenv("BEARER_TOKEN")
MATTERMOST_CHANNEL_ID = "389wx7ehk38ajc46hex5ajndxe"

# Define server order
SERVER_ORDER = [
    "BI Server", "Talend Server 1", "Talend Server 2", "Scheduler Server",
    "Repo Server", "Datanode 1", "Datanode 2", "Datanode 3",
    "Gatewaynode", "Activenode", "Standbynode", "Backup"
]

RESULT_DIR = "/home/user/airflow/maintain/maintain_refactor/result/server_visualization"
os.makedirs(RESULT_DIR, exist_ok=True)

def send_mattermost_notification(message, image_path):
    """Send a Mattermost notification with an attached image."""
    if not MATTERMOST_TOKEN or not MATTERMOST_CHANNEL_ID:
        print("❌ Mattermost token or channel ID is missing. Skipping notification.")
        return None

    url = 'https://chat.rtarf.mi.th/api/v4/posts'
    headers = {'Authorization': f'Bearer {MATTERMOST_TOKEN}', 'Content-Type': 'application/json'}

    # Upload the image to Mattermost
    file_url = 'https://chat.rtarf.mi.th/api/v4/files'
    with open(image_path, 'rb') as image_file:
        files = {'files': (image_path, image_file, 'image/png')}
        file_data = {'channel_id': MATTERMOST_CHANNEL_ID}
        
        file_response = requests.post(file_url, headers={'Authorization': f'Bearer {MATTERMOST_TOKEN}'}, files=files, data=file_data)

    if file_response.status_code != 201:
        print(f"❌ Failed to upload image: {file_response.status_code}, {file_response.text}")
        return None

    file_info = file_response.json()
    file_id = file_info.get('file_infos', [{}])[0].get('id')

    if not file_id:
        print("❌ Failed to extract file ID from Mattermost response.")
        return None

    print(f"✅ Image uploaded successfully with file_id: {file_id}")

    # Create the post with the image attached
    post_data = {
        "channel_id": MATTERMOST_CHANNEL_ID,
        "message": message,
        "file_ids": [file_id]
    }

    response = requests.post(url, headers=headers, json=post_data)

    if response.status_code == 201:
        print(f"✅ Mattermost notification sent for {image_path}")
    else:
        print(f"❌ Failed to send Mattermost message: {response.status_code}, {response.text}")

# Database Connection
db_config = {
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME')
}

encoded_password = quote_plus(db_config['password'])
connection_uri = f"postgresql+psycopg2://{db_config['user']}:{encoded_password}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
engine = create_engine(connection_uri)

def fetch_server_metrics():
    """Fetch server metrics from the database."""
    query = """
    SELECT server_name, cpu_usage_percent, total_ram_gb, used_ram_gb, used_ram_percent, 
           total_disk_gb, used_disk_gb, used_disk_percent, datetime_record
    FROM server_metrics 
    WHERE datetime_record >= NOW() - INTERVAL '7 days'
    ORDER BY datetime_record DESC;
    """
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)
        return df
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def create_table_with_border(df, ax):
    """Creates a table visualization with colored cells."""
    df = df.rename(columns={
        "server_name": "Name",
        "datetime_record": "Date",
        "cpu_usage_percent": "useCPU(%)",
        "used_ram_gb": "usedRam(GB)",
        "total_ram_gb": "maxRam(GB)",
        "used_disk_gb": "useDisk(GB)",
        "used_disk_percent": "useDisk(%)"
    })

    data = [df.columns.tolist()] + df.values.tolist()
    ax.axis('off')
    table = ax.table(cellText=data, loc='center', cellLoc='center', edges='closed')
    
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1, 1.5)
    
    header_color = '#a9c9f8'
    for key, cell in table.get_celld().items():
        if key[0] == 0:
            cell.set_facecolor(header_color)

    for row in range(1, len(data)):
        for col, val in enumerate(data[row]):
            col_name = df.columns[col]
            cell = table[row, col]
            
            if col_name == "useDisk(%)" and isinstance(val, (int, float)) and val > 70:
                cell.set_facecolor("yellow")

df = fetch_server_metrics()

if df is not None and not df.empty:
    df["datetime_record"] = pd.to_datetime(df["datetime_record"])

    # Adjust datetime to always be 08:00 AM
    df["datetime_record"] = df["datetime_record"].dt.date.astype(str) + " 08:00"
    df["datetime_record"] = pd.to_datetime(df["datetime_record"])

    # Select the most recent record per server
    df = df.loc[df.groupby("server_name")["datetime_record"].idxmax()].reset_index(drop=True)

    df["server_name"] = pd.Categorical(df["server_name"], categories=SERVER_ORDER, ordered=True)
    df = df.sort_values("server_name")

    numeric_columns = ["cpu_usage_percent", "used_ram_gb", "total_ram_gb", "used_disk_gb", "used_disk_percent"]
    df[numeric_columns] = df[numeric_columns].round(2)

    talend_group_df = df[df["server_name"].str.contains("BI Server|Talend|Scheduler|Repo", na=False)]
    hadoop_system_group_df = df[df["server_name"].str.contains("Datanode|Gatewaynode|Activenode|Standbynode|Backup", na=False)]

    fig, axs = plt.subplots(2, 1, figsize=(12, 6))

    create_table_with_border(talend_group_df, axs[0])
    axs[0].set_title('BI to Repo Stats', fontsize=12, fontweight='bold')

    create_table_with_border(hadoop_system_group_df, axs[1])
    axs[1].set_title('Datanode to Backup Stats', fontsize=12, fontweight='bold')

    plt.tight_layout()

    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M")
    output_image_file = os.path.join(RESULT_DIR, f'server_stats_visualization_{current_date}.png')
    plt.savefig(output_image_file, dpi=300)
    
    print("✅ Plot saved:", output_image_file)

    # Send Mattermost notification
    message = f"📊 **Server Resource Usage Report**\n🕒 {current_date}"
    send_mattermost_notification(message, output_image_file)

    plt.show()

else:
    print("No data retrieved from the database.")
