# Code for ETL operations on Country-GDP data
import os
from io import StringIO
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
# from icecream import ic


def log_progress(message):
    """Logs the given message to a log file.

    This function logs the mentioned message of a given stage of the
    code execution to a log file. If the log directory or file doesn't exist,
    it will attempt to create them. Function returns nothing.
    """

    log_dir = './logs'
    log_file = os.path.join(log_dir, 'code_log.txt')

    try:
        # Ensure the log directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Open the log file in append mode and write the log
        with open(log_file, 'a') as f:
            f.write(f'{datetime.now()}: {message}\n')
    except PermissionError:
        print("Error: Insufficient permissions to write to the log file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



def extract(url, table_attribs):
    """Extracts data from a webpage and returns it as a DataFrame.

    This function fetches the webpage content, parses the HTML to locate the
    specified table, and converts it into a DataFrame. It logs the progress
    and handles exceptions gracefully.

    Args:
        url (str): The URL of the webpage to extract data from.
        table_attribs (str): The attribute of the target table to locate it.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame.

    Raises:
        ValueError: If data extraction fails or table is not found.
    """
    try:
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('span', string=table_attribs)
        
        if table is None:
            raise ValueError(f"Table with attributes '{table_attribs}' not found on the webpage.")

        # Extract the table and convert to DataFrame
        table = table.find_next('table')
        if table is None:
            raise ValueError("No table found after the specified span.")

        df = pd.read_html(StringIO(str(table)))[0]

        # Log progress
        log_progress('Data extraction complete. Initiating Transformation process')

        return df

    except requests.exceptions.RequestException as e:
        log_progress(f"Network error: {e}")
        raise RuntimeError(f"Failed to fetch URL: {url}. Check your connection or the URL.") from e

    except ValueError as e:
        log_progress(f"Data extraction error: {e}")
        raise

    except Exception as e:
        log_progress(f"Unexpected error during extraction: {e}")
        raise RuntimeError("An unexpected error occurred during data extraction.") from e
    


def transform(df, csv_path):
    """Transforms the DataFrame by adding market capitalization columns in different currencies.

    This function reads exchange rate information from a CSV file and adds columns
    to the DataFrame for market capitalization in GBP, EUR, INR, and PKR.

    Args:
        df (pd.DataFrame): The input DataFrame containing 'Market cap (US$ billion)' column.
        csv_path (str): Path to the CSV file with exchange rates.

    Returns:
        pd.DataFrame: The transformed DataFrame with additional columns.

    Raises:
        ValueError: If required columns are missing or invalid.
        FileNotFoundError: If the exchange rate CSV file is not found.
        KeyError: If the necessary exchange rates are not available in the CSV.
    """
    try:
        # Validate input DataFrame
        if 'Market cap (US$ billion)' not in df.columns:
            raise ValueError("The input DataFrame is missing the 'Market cap (US$ billion)' column.")

        # Load exchange rate data
        try:
            exchange_rate = pd.read_csv(csv_path, index_col=0).to_dict()['Rate']
        except FileNotFoundError:
            log_progress(f"Exchange rate file not found: {csv_path}")
            raise FileNotFoundError(f"Exchange rate file '{csv_path}' not found.")
        except KeyError:
            log_progress("The exchange rate file is missing the required 'Rate' column.")
            raise KeyError("The exchange rate file is missing the required 'Rate' column.")

        # Add transformed columns
        currencies = ['GBP', 'EUR', 'INR', 'PKR']
        for currency in currencies:
            if currency not in exchange_rate:
                log_progress(f"Exchange rate for {currency} not found in the CSV file.")
                raise KeyError(f"Exchange rate for {currency} is missing in the exchange rate file.")

            df[f'MC_{currency}_Billion'] = round(df['Market cap (US$ billion)'] * exchange_rate[currency], 2)

        # Log success
        log_progress("Data transformation complete. Initiating Loading process.")

        return df

    except Exception as e:
        log_progress(f"Data transformation failed: {e}")
        raise RuntimeError(f"An error occurred during data transformation: {e}") from e



def load_to_csv(df, output_path):
    """Saves the given DataFrame as a CSV file at the specified path.

    This function saves the final data frame as a CSV file. If the
    output directory doesn't exist, it will attempt to create it.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The full path (including filename) where the CSV should be saved.

    Returns:
        None

    Raises:
        ValueError: If the DataFrame is empty or invalid.
        IOError: If there are issues writing the file.
    """
    try:
        # Validate DataFrame
        if df is None or df.empty:
            raise ValueError("The provided DataFrame is empty or None.")
        
        # Ensure the output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the DataFrame to CSV
        df.to_csv(output_path, index=False)

        # Log progress
        log_progress(f"Data successfully saved to {output_path}")

    except ValueError as e:
        log_progress(f"Data saving error: {e}")
        raise

    except IOError as e:
        log_progress(f"File writing error: {e}")
        raise RuntimeError(f"Failed to save CSV to {output_path}. Check file permissions or disk space.") from e

    except Exception as e:
        log_progress(f"Unexpected error: {e}")
        raise RuntimeError(f"An unexpected error occurred while saving CSV: {e}") from e


def load_to_db(df, sql_connection, table_name):
    """Saves the given DataFrame to a database table.

    This function saves the final data frame to a database table with the provided
    name. If the DataFrame is empty or there are database issues, it handles
    exceptions gracefully.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        sql_connection: A SQLAlchemy database connection or engine.
        table_name (str): The name of the database table.

    Returns:
        None

    Raises:
        ValueError: If the DataFrame is empty or None.
        RuntimeError: For any database-related issues.
    """
    try:
        # Validate the DataFrame
        if df is None or df.empty:
            raise ValueError("The provided DataFrame is empty or None.")

        # Save the DataFrame to the database
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

        # Log progress
        log_progress(f"Data successfully loaded to the database table '{table_name}'. Executing queries.")

    except ValueError as e:
        log_progress(f"Data loading error: {e}")
        raise

    except Exception as e:
        log_progress(f"Unexpected error while loading data to the database: {e}")
        raise RuntimeError(f"An unexpected error occurred while saving data to the table '{table_name}': {e}") from e


def run_query(query_statement, sql_connection):
    """Runs a query on the database and prints the output.

    This function executes the given query on the connected database table
    and returns the results. If an error occurs, it logs the error and
    re-raises the exception.

    Args:
        query_statement (str): The SQL query to be executed.
        sql_connection: The database connection object.

    Returns:
        list: The query results as a list of tuples.

    Raises:
        RuntimeError: For any database-related issues.
    """
    try:
        # Create a cursor object
        cursor = sql_connection.cursor()

        # Execute the query
        cursor.execute(query_statement)

        # Fetch the results
        result = cursor.fetchall()

        # Log progress
        log_progress("Query executed successfully. Process complete.")

        return result

    except sql_connection.Error as e:
        log_progress(f"Database error: {e}")
        raise RuntimeError(f"An error occurred while executing the query: {e}") from e

    except Exception as e:
        log_progress(f"Unexpected error during query execution: {e}")
        raise RuntimeError(f"An unexpected error occurred while executing the query: {e}") from e

    finally:
        # Ensure the cursor is closed
        if 'cursor' in locals() and cursor:
            cursor.close()


if __name__ == '__main__':
    try:
        # Input and output paths
        url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
        output_csv_path = './output/Largest_banks_data.csv'
        database_name = './output/Banks.db'
        table_name = 'Largest_banks'
        
        # Log the start of the ETL process
        log_progress('Preliminaries complete. Initiating ETL process')

        # Extraction
        try:
            df = extract(url, 'By market capitalization')
        except Exception as e:
            log_progress(f"Extraction failed: {e}")
            raise RuntimeError("ETL process halted during extraction.") from e

        # Transformation
        try:
            transform(df, './input/exchange_rate.csv')
        except Exception as e:
            log_progress(f"Transformation failed: {e}")
            raise RuntimeError("ETL process halted during transformation.") from e

        # Load to CSV
        try:
            load_to_csv(df, output_csv_path)
        except Exception as e:
            log_progress(f"Failed to save data to CSV: {e}")
            raise RuntimeError("ETL process halted during CSV load.") from e

        # Load to Database
        try:
            with sqlite3.connect(database_name) as conn:
                load_to_db(df, conn, table_name)

                # Query 1: Select all records
                try:
                    result = run_query('SELECT * FROM Largest_banks', conn)
                    print("All Records:")
                    for row in result:
                        print(row)
                except Exception as e:
                    log_progress(f"Query 1 failed: {e}")
                    raise

                # Query 2: Calculate the average market capitalization
                try:
                    result = run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
                    print("\nAverage Market Capitalization (in GBP Billion):")
                    print(result[0][0])
                except Exception as e:
                    log_progress(f"Query 2 failed: {e}")
                    raise

                # Query 3: Fetch the first five bank names
                try:
                    result = run_query('SELECT "Bank name" FROM Largest_banks LIMIT 5', conn)
                    print("\nFirst 5 Bank Names:")
                    for row in result:
                        print(row[0])
                except Exception as e:
                    log_progress(f"Query 3 failed: {e}")
                    raise
        except Exception as e:
            log_progress(f"Database load or query execution failed: {e}")
            raise RuntimeError("ETL process halted during database operations.") from e

        log_progress('ETL process completed successfully.')

    except Exception as e:
        # Log any critical failure in the ETL pipeline
        log_progress(f"ETL process failed: {e}")
        print(f"Critical Error: {e}")