import pandas as pd
import sqlite3

#Local path of XLSX filel̥
excel_file_path = 'data/SDG_12_30_v01_r00.xlsx'

#Name of sheets in the XLSX file
sheet_name = 'SDG_12_30'

# Read the Excel file into a pandas DataFrame
# I put hedear as a 9 because there is some basic info related unit of emmision in the the row number 0 to 8.
df = pd.read_excel(excel_file_path,sheet_name=sheet_name,header=9)

# Now I delete the row 0 and 1 because i dont need that in my comparision
rows_to_delete = [0, 1]
df = df.drop(rows_to_delete)

db_name ='data/Population_VS_Emmision_Data.sqlite'
# Connect to SQLite database (or create a new one if it doesn't exist)
conn = sqlite3.connect(db_name)

# Write the DataFrame to a SQLite table
df.to_sql('Europe Emission', conn, index=False, if_exists='replace')

# Close the database connection
conn.close()


# -----------------------------------------------------------------

#Local path of XLSX filel̥
excel_file_path = 'data/pupulation_csv.xlsx'

#Name of sheets in the XLSX file
sheet_name = 'Sheet 1'

# Read the Excel file into a pandas DataFrame
# I put hedear as a 9 because there is some basic info related unit of emmision in the the row number 0 to 8.
df = pd.read_excel(excel_file_path,sheet_name=sheet_name,header=7)

# Now I delete the row 0 and 1 because i dont need that in my comparision
rows_to_delete = [0, 1,2,3,52,53,54,55,56,57,58,59,60,61]
# Identify and drop unnamed columns
unnamed_columns = [col for col in df.columns if 'Unnamed' in col]
df = df.drop(columns=unnamed_columns, errors='ignore')
df = df.drop(rows_to_delete)

db_name ='data/Population_VS_Emmision_Data.sqlite'
# Connect to SQLite database (or create a new one if it doesn't exist)
conn = sqlite3.connect(db_name)

# Write the DataFrame to a SQLite table
df.to_sql('Europe Populaton', conn, index=False, if_exists='replace')

# Close the database connection
conn.close()

print("Conversion successful!")