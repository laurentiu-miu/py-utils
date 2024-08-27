# Download BVB history data
Download the data from [BVB LINK](https://www.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=UZT) and upload it to a postgres database.

### Create the database table
```sql
CREATE TABLE istoric_tranzactionare (
    id SERIAL PRIMARY KEY,
    data DATE,
    piata VARCHAR(10),
    tranzactii INTEGER,
    volum INTEGER,
    valoare NUMERIC,
    pret_deschidere NUMERIC,
    pret_minim NUMERIC,
    pret_maxim NUMERIC,
    pret_mediu NUMERIC,
    pret_inchidere NUMERIC,
    variatie NUMERIC,
    simbol VARCHAR(10)
);
```

### Install requierments for upload and download
```sh
pip3 install -r requirements.txt
```

### Run script to download the data from bvb and upload to database
```sh
python script.py
python fetch_data.py
```
