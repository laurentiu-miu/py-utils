import os
import time
import shutil
import psycopg2
import csv
import json
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from webdriver_manager.chrome import ChromeDriverManager

# Load configuration from JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

# Extract configuration details
db_config = config["database"]
symbols = config["symbols"]
download_dir = config["download_directory"]

# Set up Chrome options
chrome_options = Options()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_dir,  # Change default directory for downloads
    "download.prompt_for_download": False,  # Disable download prompt
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Connect to PostgreSQL database using details from the configuration file
conn = psycopg2.connect(
    dbname=db_config["dbname"],
    user=db_config["user"],
    password=db_config["password"],
    host=db_config["host"],
    port=db_config["port"]
)
cur = conn.cursor()

# Set up the WebDriver using WebDriverManager with Service
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

for simbol in symbols:
    try:
        # Open the webpage for each symbol
        url = f"https://www.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s={simbol}"
        driver.get(url)

        # Wait for the page to load fully
        time.sleep(5)  # Adjust the sleep time as needed

        # Find the "Tranzactionare" tab by input value and click it
        try:
            tranzactionare_tab = driver.find_element(By.XPATH, "//input[@value='Tranzactionare']")
            tranzactionare_tab.click()
        except NoSuchElementException:
            print(f"Tab 'Tranzactionare' not found for symbol: {simbol}. Skipping to next symbol.")
            continue

        # Wait for the content under the "Tranzactionare" tab to load
        time.sleep(3)  # Adjust the sleep time as needed

        # Now find the "CSV" button and click it using JavaScript to avoid interception
        try:
            csv_button = driver.find_element(By.XPATH, "//button[contains(@class, 'buttons-csv')]")
            driver.execute_script("arguments[0].click();", csv_button)
        except NoSuchElementException:
            print(f"CSV button not found for symbol: {simbol}. Skipping to next symbol.")
            continue

        # Wait for the file to download
        time.sleep(5)  # Adjust the sleep time as needed

        # Find the downloaded file in the download directory
        filename = f"Istoric tranzactionare {simbol}.csv"
        filepath = os.path.join(download_dir, filename)

        if not os.path.exists(filepath):
            print(f"File for {simbol} not found. Skipping.")
            continue

        # Read the CSV file and insert data into the database if not already exists
        with open(filepath, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header row
            for row in reader:
                # Convert the date from DD.MM.YYYY to YYYY-MM-DD format
                data_date = datetime.strptime(row[0], '%d.%m.%Y').strftime('%Y-%m-%d')

                # Check if the record already exists in the database
                cur.execute("""
                    SELECT 1 FROM istoric_tranzactionare 
                    WHERE data = %s AND simbol = %s
                """, (data_date, simbol))
                exists = cur.fetchone()

                if exists:
                    print(f"Data for {simbol} on {data_date} already exists in the database. Skipping insert.")
                    continue

                # Insert data into the database
                cur.execute("""
                    INSERT INTO istoric_tranzactionare 
                    (data, piata, tranzactii, volum, valoare, pret_deschidere, pret_minim, pret_maxim, pret_mediu, pret_inchidere, variatie, simbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    data_date,  # Data
                    row[1],  # Piata
                    int(row[2].replace('.', '').replace(',', '')),  # Tranzactii
                    int(row[3].replace('.', '').replace(',', '')),  # Volum
                    float(row[4].replace('.', '').replace(',', '.')),  # Valoare
                    float(row[5].replace(',', '.')),  # Pret deschidere
                    float(row[6].replace(',', '.')),  # Pret minim
                    float(row[7].replace(',', '.')),  # Pret maxim
                    float(row[8].replace(',', '.')),  # Pret mediu
                    float(row[9].replace(',', '.')),  # Pret inchidere
                    float(row[10].replace(',', '.')),  # Variatie
                    simbol  # Simbol
                ))

            conn.commit()  # Commit the transaction

        # After processing the file, delete it
        os.remove(filepath)
        print(f"File for {simbol} processed and deleted.")

    except Exception as e:
        print(f"An error occurred for symbol {simbol}: {str(e)}")
        continue

# Close the browser and the database connection after processing all symbols
driver.quit()
cur.close()
conn.close()

