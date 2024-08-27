import psycopg2
import pandas as pd
import json

def fetch_data_from_db(symbols):
    try:
        # Load configuration from the config.json file
        with open('config.json') as config_file:
            config = json.load(config_file)

        # Extract database configuration details
        db_config = config["database"]

        # Connect to PostgreSQL database using details from the configuration file
        connection = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Prepare the query to fetch data only for the specified symbols
        query = """
        SELECT * FROM public.istoric_tranzactionare
        WHERE simbol = ANY(%s);
        """
        
        # Execute the query with the symbols as parameters
        cursor.execute(query, (symbols,))

        # Fetch all rows from the executed query
        records = cursor.fetchall()

        # Get column names
        colnames = [desc[0] for desc in cursor.description]

        # Create a pandas DataFrame from the fetched data
        df = pd.DataFrame(records, columns=colnames)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Load configuration from the config.json file
    with open('config.json') as config_file:
        config = json.load(config_file)

    # Get the symbols to fetch from the config
    symbols_to_fetch = config["symbols_to_fetch"]

    # Fetch data only for the specified symbols
    data = fetch_data_from_db(symbols_to_fetch)

    if data is not None:
        # Display the data for analysis
        print(data)
        # Optionally, save it to a file for further use
        data.to_csv('output.csv', index=False)

