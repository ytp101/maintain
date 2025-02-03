import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import plotly.graph_objects as go
import plotly.io as pio
import requests

def create_db_connection(username, password, host, port, database):
    connection_uri = URL.create(
        drivername='postgresql+psycopg2',
        username=username,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return create_engine(connection_uri)

def fetch_lock_data(conn):
    select_stm = """
    SELECT DISTINCT ON (hl_table) hl_db, hl_table, hl_agent_info
    FROM HIVE_LOCKS
    WHERE hl_table IS NOT NULL AND hl_agent_info != 'Unknown'
    ORDER BY hl_table, hl_agent_info;
    """
    df = pd.read_sql_query(select_stm, conn)
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'Index'}, inplace=True)
    return df

def create_table_figure(df):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(df.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[df.Index, df.hl_db, df.hl_table, df.hl_agent_info],
                   fill_color='lavender',
                   align='left'),
        columnwidth=[30, 115, 300, 270]
    )])
    fig.update_layout(
        title="Database Table Contents Lock",
        height=800,
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig

def save_figure(fig, image_path):
    pio.write_image(fig, image_path)

def send_line_notification(message, image_path, token):
    url = 'https://notify-api.line.me/api/notify'
    headers = {'Authorization': 'Bearer ' + token}
    with open(image_path, 'rb') as image_file:
        files = {'imageFile': image_file}
        data = {'message': message}
        response = requests.post(url, headers=headers, data=data, files=files)
    return response

def main():
    # Database credentials
    DB_CONFIG = {
        'username': 'hive',
        'password': 'eex9Phae',
        'host': '10.104.4.19',
        'port': 5432,
        'database': 'metastore'
    }

    # LINE Notify token
    LINE_TOKEN = 'DbeEYA3RPe5sfnsXnN1OcyZhxjBJRCperJIldgGmQ7G'

    # Create connection and fetch data
    engine = create_db_connection(**DB_CONFIG)
    with engine.connect() as conn:
        df = fetch_lock_data(conn)

    print(f"Number of Lock Rows: {len(df)}")
    print(df)

    # Create and save figure
    fig = create_table_figure(df)
    image_path = 'table_visualization.png'
    save_figure(fig, image_path)

    # Send notification
    message = "Database Table Contents Lock:"
    response = send_line_notification(message, image_path, LINE_TOKEN)
    print(f"LINE Notify response: {response.status_code}, {response.text}")

if __name__ == "__main__":
    main()
