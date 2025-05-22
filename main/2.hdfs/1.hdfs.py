import paramiko
import os
import re
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def parse_hdfs_report(output):
    """Extract DFS Used and DFS Remaining from hdfs dfsadmin -report output."""
    dfs_used_bytes = dfs_remaining_bytes = None

    for line in output.splitlines():
        used_match = re.search(r"DFS Used:\s+([\d,]+)\s+B", line)
        remaining_match = re.search(r"DFS Remaining:\s+([\d,]+)\s+B", line)

        if used_match:
            dfs_used_bytes = used_match.group(1).replace(',', '')
        if remaining_match:
            dfs_remaining_bytes = remaining_match.group(1).replace(',', '')

        if dfs_used_bytes and dfs_remaining_bytes:
            break

    if dfs_used_bytes is None or dfs_remaining_bytes is None:
        raise ValueError("Failed to extract DFS usage data. Check HDFS report output format.")

    return dfs_used_bytes, dfs_remaining_bytes

def execute_ssh_command(client, command):
    """Execute a command over SSH and return the output."""
    stdin, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode('utf-8').strip()
    error = stderr.read().decode('utf-8').strip()

    if error:
        raise Exception(f"SSH Command Error: {error}")

    return output

def fetch_hdfs_usage():
    """Connect via SSH, authenticate with Kerberos if required, and collect HDFS disk usage."""
    
    # Load from .env
    SERVER_IP = os.getenv("SERVER_IP")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    KEYTAB_PATH = os.getenv("KEYTAB_PATH")
    PRINCIPAL = os.getenv("PRINCIPAL")

    if not SERVER_IP or not USERNAME:
        raise ValueError("Missing SERVER_IP or USERNAME in .env file.")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # SSH Connection
        if PASSWORD:
            client.connect(SERVER_IP, username=USERNAME, password=PASSWORD)
        else:
            client.connect(SERVER_IP, username=USERNAME)

        # Authenticate with Kerberos if a keytab is provided
        if KEYTAB_PATH and PRINCIPAL:
            kinit_cmd = f'kinit -kt {KEYTAB_PATH} {PRINCIPAL}'
            execute_ssh_command(client, kinit_cmd)

        # Execute HDFS report command
        hdfs_report_cmd = 'hdfs dfsadmin -report'
        output = execute_ssh_command(client, hdfs_report_cmd)

        # Parse the output
        dfs_used_bytes, dfs_remaining_bytes = parse_hdfs_report(output)

        # Convert from bytes to terabytes (TB)
        dfs_used_tb = round(int(dfs_used_bytes) / (1024 ** 4), 2)
        dfs_remaining_tb = round(int(dfs_remaining_bytes) / (1024 ** 4), 2)

        print(f"✅ DFS Used: {dfs_used_tb} TB, DFS Remaining: {dfs_remaining_tb} TB")

        return dfs_used_tb, dfs_remaining_tb

    except paramiko.AuthenticationException:
        print("❌ Authentication failed. Check Kerberos or SSH credentials.")
    except paramiko.SSHException as e:
        print(f"❌ SSH error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        client.close()

def plot_pie_chart(dfs_used_tb, dfs_remaining_tb):
    """Generate and display a pie chart for HDFS usage."""
    labels = ['DFS Used', 'DFS Remaining']
    sizes = [dfs_used_tb, dfs_remaining_tb]
    colors = ['red', 'green']
    
    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
            startangle=140, wedgeprops={'edgecolor': 'black'})
    plt.title("HDFS Storage Usage (TB)")
    plt.axis('equal')

    # Save the plot
    chart_path = "hdfs_usage_piechart.png"
    plt.savefig(chart_path)
    print(f"✅ Pie chart saved as {chart_path}")

if __name__ == "__main__":
    result = fetch_hdfs_usage()
    if result:
        plot_pie_chart(*result)
