from sqlalchemy import create_engine
import pandas as pd
import time

# dummy database url for connection
# DATABASES = {'default': dj_database_url.parse("postgres://username:password@hostname:port/database_name") }

# Timer Initiated
start_time = time.time()

# Connect to Postgres
DATABASE_URL = "postgresql://kmekurhs:455_uJLxTrbRj0eneYNGsQO5EnIxWdCz@snuffleupagus.db.elephantsql.com/kmekurhs"

TABLE_NAME = "testing"

File = "./static/testing_data.csv"


# Set up PostgreSQL connection
def get_connection():
    return create_engine(url=DATABASE_URL)


# Load data from CSV file into Pandas DataFrame
def get_csv():
    return pd.read_csv(File)


try:
    # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
    engine = get_connection()
    print(f"Connection to the {engine} created successfully.")

except Exception as ex:
    print("Connection could not be made due to the following error: \n", ex)

try:
    # GET THE REQUIRED CSV FILE and SHOW DATA OF FILE
    csv_f = get_csv()
    print(f"CSV File data loaded successfully.")

except Exception as ex:
    print("CSV File is not loaded Successfully: \n", ex)

print("Loading....")

try:
    # Get the columns of the existing table
    existing_columns = pd.read_sql_table(TABLE_NAME, engine).columns
    existing_columns_count = existing_columns.__len__()

    if existing_columns_count == 0:
        print(f"Total Columns in Table = {existing_columns_count}, ! ERROR")
    else:
        print("Uploading")
        try:
            # Filter the DataFrame to only include columns that exist in the table
            csv_f_filtered = csv_f.loc[:, csv_f.columns.isin(existing_columns)]
            # Append the filtered data to the existing table
            csv_f_filtered.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
            print(f"Congratulations! CSV uploaded to database successfully")
        except Exception as ex:
            print("There is some error in uploading of data to database \n", ex)

except:
    # Table Not Found, Creating New Table
    print(f"{TABLE_NAME} table not found. So, continue on creating new table")
    try:
        # Insert data into PostgreSQL table using SQLAlchemy
        csv_f.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        print(f"Congratulations! CSV uploaded to database successfully")
    except Exception as ex:
        print("There is some error in uploading of data to database \n", ex)


# Stop Timer
end_time = time.time()

# Close connection
engine.dispose()

# Verifying Process Time
duration = end_time - start_time
print(f"Process took {duration:.2f} seconds to complete.")
