import pandas as pd
import paramiko
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from urllib.parse import quote_plus

db_config = {
    'username': 'Django',
    'password': '@45151BA2fax',  # Contains special character (@)
    'host': '10.104.5.107',
    'port': 5432,
    'database': 'maintain',
    'table_name': 'server_metrics'
}

# URL-encode the password to prevent errors
encoded_password = quote_plus(db_config['password'])

# Correct PostgreSQL connection string
connection_uri = f"postgresql://{db_config['username']}:{encoded_password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Create SQLAlchemy engine
engine = create_engine(connection_uri)

class Server:
    def __init__(self, name, ip, username, password):
        self.name = name
        self.ip = ip
        self.username = username
        self.password = password
        self.ssh = None

    def connect(self):
        """Establish an SSH connection."""
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.ip, username=self.username, password=self.password)
        except Exception as e:
            print(f"Error connecting to {self.name}: {e}")
            self.ssh = None

    def execute_command(self, command):
        """Execute a command over SSH and return the output."""
        if not self.ssh:
            return None
        try:
            stdin, stdout, stderr = self.ssh.exec_command(command)
            return stdout.read().decode().strip()
        except Exception as e:
            print(f"Error executing command on {self.name}: {e}")
            return None

    def get_system_info(self):
        """Collect system information from the server."""
        self.connect()
        if not self.ssh:
            return self.name, self.ip, "Connection Failed", "N/A", "N/A", "N/A", "N/A"

        
        cpu_usage = self.execute_command("top -bn1 | awk 'NR==3{print $2}'")
        
        total_ram = self.execute_command("awk '/MemTotal/ {print $2 / 1024 / 1024}' /proc/meminfo")
        used_ram = self.execute_command("awk '/MemTotal/ {total=$2} /MemAvailable/ {available=$2} END {print (total - available) / 1024 / 1024}' /proc/meminfo")

        total_disk = self.execute_command("df -BG --total | grep total | awk '{print $2}' | tr -d 'G'")
        used_disk = self.execute_command("df -BG --total | grep total | awk '{print $3}' | tr -d 'G'")


        self.close_connection()

        return self.name, self.ip, cpu_usage, total_ram, used_ram, total_disk, used_disk

    def close_connection(self):
        """Close the SSH connection."""
        if self.ssh:
            self.ssh.close()
            self.ssh = None

def write_to_db(df):
    """Write the collected data to a database."""
    try:
        df.to_sql(name='server_info', con=engine, if_exists='append', index=False)
        print("Data has been written to the database successfully.")
    except Exception as e:
        print(f"Error writing data to the database: {e}")

# Server details
server_list = [
    {"name": "BI Server", "ip": "10.104.5.86", "username": "user", "password": "Wyw7PLwC"},
    {"name": "Talend Server 1", "ip": "10.104.5.87", "username": "user", "password": "cFD6UEnn"},
    {"name": "Talend Server 2", "ip": "10.104.5.88", "username": "user", "password": "6mJf7Pmr"},
    {"name": "Scheduler Server", "ip": "10.104.5.89", "username": "user", "password": "SWE8Ufx2"},
    {"name": "Repo Server", "ip": "10.104.5.80", "username": "user", "password": "8yu43TSM"},
    {"name": "Datanode 1", "ip": "10.104.4.20", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Datanode 2", "ip": "10.104.4.21", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Datanode 3", "ip": "10.104.4.22", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Gatewaynode", "ip": "10.104.4.19", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Activenode", "ip": "10.104.4.252", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Standbynode", "ip": "10.104.4.60", "username": "rtarf_admin", "password": "4xWRwSKaI6"},
    {"name": "Backup", "ip": "10.104.5.161", "username": "root", "password": "Bkdop@6584"}
]

def main():
    results = []
    with ThreadPoolExecutor(max_workers=12) as executor:
        future_to_server = {executor.submit(Server(**server).get_system_info): server for server in server_list}
        
        for future in as_completed(future_to_server):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error collecting data: {e}")


    df = pd.DataFrame(results, columns=["server_name", "ip", "cpu_usage_percent", "total_ram_gb", "used_ram_gb", "total_disk_gb", "used_disk_gb"])
    
    numeric_cols = ["cpu_usage_percent", "total_ram_gb", "used_ram_gb", "total_disk_gb", "used_disk_gb"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce") 

    df["used_ram_percent"] = ((df["used_ram_gb"] / df["total_ram_gb"]) * 100).round(2)
    df["used_disk_percent"] = (df["used_disk_gb"] / df["total_disk_gb"]) * 100

    df["used_ram_percent"] = df["used_ram_percent"].replace([float('inf'), -float('inf')], 0).fillna(0)
    df["used_disk_percent"] = df["used_disk_percent"].fillna(0).round(2)

    df['datetime_record'] = pd.Timestamp.now()

    print(df)
    df.to_sql(name=db_config['table_name'], con=engine, if_exists='append', index=False)
    
    print("Data successfully written to the database.")

if __name__ == "__main__":
    main()