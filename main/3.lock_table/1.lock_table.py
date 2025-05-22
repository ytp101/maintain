import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import plotly.graph_objects as go
import plotly.io as pio
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Secure database credentials
DB_CONFIG = {
    'username': os.getenv('HL_TABLE_USER'),
    'password': os.getenv('HL_TABLE_PASSWORD'),
    'host': os.getenv('HL_TABLE_IP'),
    'port': int(os.getenv('HL_TABLE_PORT', 5432)),
    'database': os.getenv('HL_TABLE_DB')
}

MATTERMOST_TOKEN = os.getenv("BEARER_TOKEN")

# Define the relative path for saving results
RESULT_DIR = "/home/user/airflow/maintain/maintain_refactor/result/locktable"

# Ensure the directory exists
os.makedirs(RESULT_DIR, exist_ok=True)

def create_db_connection(username, password, host, port, database):
    """Create a secure connection to PostgreSQL."""
    try:
        connection_uri = URL.create(
            drivername='postgresql+psycopg2',
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        engine = create_engine(connection_uri)
        print("✅ Database connection established.")
        return engine
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_lock_data(conn):
    """Retrieve locked tables from Hive Metastore."""
    select_stm = """
    SELECT hl_db, hl_table, hl_agent_info
    FROM hive_locks
    WHERE hl_table IS NOT NULL AND hl_agent_info != 'Unknown'
    GROUP BY hl_db, hl_table, hl_agent_info
    ORDER BY hl_table ASC, hl_agent_info ASC;
    """
    df = pd.read_sql_query(select_stm, conn)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Index'}, inplace=True)
    return df

def create_table_figure(df):
    """Create an interactive table figure using Plotly."""
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[
            df.get('Index', range(len(df))), 
            df['hl_db'], df['hl_table'], df['hl_agent_info']
        ],
        fill_color='lavender', align='left'),
        columnwidth=[30, 115, 300, 270]
    )])
    fig.update_layout(
        title="Database Table Contents Lock",
        height=800,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig

def save_figure(fig):
    """Save the table figure as an image with a timestamped filename."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Format: YYYY-MM-DD_HH-MM-SS
    image_path = os.path.join(RESULT_DIR, f"locktable_{timestamp}.png")
    
    pio.write_image(fig, image_path)
    print(f"✅ Image saved at {image_path}")
    
    return image_path  # Return the generated filename

def send_mattermost_notification(message, image_path, token, channel_id):
    """Send a notification to Mattermost with the image."""
    if not token or not channel_id:
        print("❌ Mattermost token or channel ID is missing. Skipping notification.")
        return None

    url = 'https://chat.rtarf.mi.th/api/v4/posts'
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    # First, upload the image to Mattermost
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

    # Now, create the post with the image attached
    post_data = {
        "channel_id": channel_id,
        "message": message,
        "file_ids": [file_id]  # Attach uploaded file
    }

    response = requests.post(url, headers=headers, json=post_data)

    if response.status_code == 201:
        print("✅ Message and image sent to Mattermost successfully.")
    else:
        print(f"❌ Failed to send Mattermost message: {response.status_code}, {response.text}")

    return response


def main():
    engine = create_db_connection(**DB_CONFIG)
    if not engine:
        return

    with engine.connect() as conn:
        df = fetch_lock_data(conn)

    print(f"Number of Lock Rows: {len(df)}\n", df)

    fig = create_table_figure(df)
    image_path = save_figure(fig)  # Get the saved image path

    MATTERMOST_CHANNEL_ID = "389wx7ehk38ajc46hex5ajndxe"

    send_mattermost_notification("Database Table Contents Lock:", image_path, MATTERMOST_TOKEN, MATTERMOST_CHANNEL_ID)

if __name__ == "__main__":
    main()
