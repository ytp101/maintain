import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
# excel ไม่ได้เอา เวลามาด้วยนะ 
# Read data from Excel file
file_path = '/home/user/airflow/maintain/maintain/maintain/server_stats.xlsx'

# Assuming there are two sheets named 'Talend_Group' and 'Hadoop_System_Group'
Talend_Group_df = pd.read_excel(file_path, sheet_name='Talend_Group')
Hadoop_System_Group_df = pd.read_excel(file_path, sheet_name='Hadoop_System_Group')
Talend_Group_df['Date'] = pd.to_datetime(Talend_Group_df['Date'], format="%d-%m-%Y %H:%M", errors="coerce")
Hadoop_System_Group_df['Date'] = pd.to_datetime(Hadoop_System_Group_df['Date'], format="%d-%m-%Y %H:%M", errors="coerce")

# Get current date and time
max_date = Talend_Group_df['Date'].max()
# Filter data for the current date
Talend_Group_df = Talend_Group_df[Talend_Group_df['Date'] == max_date]
Hadoop_System_Group_df = Hadoop_System_Group_df[Hadoop_System_Group_df['Date'] == max_date]

Talend_Group_df['Date'] = Talend_Group_df['Date'].dt.strftime('%Y-%m-%d %H:%M')
Hadoop_System_Group_df['Date'] = Hadoop_System_Group_df['Date'].dt.strftime('%Y-%m-%d %H:%M')

# Debug
# Raname Talend Group df becuase hte useRam(%) is have " useRam(%)"
Talend_Group_df.rename(columns={
    ' useRam(%)': 'useRam(%)',
}, inplace=True)
print("Talend_Group_df")
print(Talend_Group_df)
print(Talend_Group_df.info())
print("--------------------------")
print("Hadoop_System_Group_df")
print(Hadoop_System_Group_df)
print(Hadoop_System_Group_df.info())

def create_table_with_border(df, ax):
    # Convert dataframe to list of lists for table creation
    data = [df.columns.tolist()] + df.values.tolist()
    
    # Hide axes
    ax.axis('off')
    
    # Create the table with closed edges for borders
    table = ax.table(cellText=data, loc='center', cellLoc='center', edges='closed')
    
    # Set font size to 8
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    
    # Set column widths to accommodate date and time
    table.scale(1, 1.5)
    
    # Set header color
    header_color = '#a9c9f8'  # You can choose any color you prefer
    for key, cell in table.get_celld().items():
        if key[0] == 0:
            cell.set_facecolor(header_color)

    # Define color mappings based on conditions
    color_map = {
    'useCPU(%)': lambda x: 'red' if x > 80 else ('yellow' if x > 70 else 'white'),
    'useRam(%)': lambda x: 'red' if x > 80 else ('yellow' if x > 70 else 'white'),
    'useDisk(%)': lambda x: 'red' if x > 80 else ('yellow' if x > 70 else 'white')
    }

    # Apply color to cells based on conditions
    for row in range(1, len(data)):
        print(data)
        for col, val in enumerate(data[row]):
            col_name = df.columns[col]
            cell = table[row, col]
            
            if col_name in color_map:
                # Debug log: Print column name, value, and determined color
                color = color_map[col_name](val)
                print(f"Column: {col_name}, Value: {val}, Color: {color}")
                
                # Set the cell color
                cell.set_facecolor(color)
        print("----------------")


# Create subplots for each worksheet
fig, axs = plt.subplots(2, 1, figsize=(12, 6))

# Create table-like visualization for Talend_Group_df with borders and cell highlighting
create_table_with_border(Talend_Group_df, axs[0])
axs[0].set_title('BI to Repo Stats', fontsize=12, fontweight='bold')

# Create table-like visualization for Hadoop_System_Group_df with borders and cell highlighting
create_table_with_border(Hadoop_System_Group_df, axs[1])
axs[1].set_title('Datanode to Backup Stats', fontsize=12, fontweight='bold')

# Adjust layout
plt.tight_layout()

# Save the plot as an image file
now = datetime.now()
current_date = now.strftime("%d-%m-%Y %H:%M")
output_image_file = f'/home/user/airflow/maintain/maintain/maintain/server_stats_visualization_{current_date}.png'
plt.savefig(output_image_file, dpi=300)

print("Plot saved as:", output_image_file)
