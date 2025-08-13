# BVB CSV Downloader & Ingestor

This script automates downloading **trading history CSVs** from the Bucharest Stock Exchange (BVB) for selected tickers, then **ingests** the data into a **PostgreSQL** table while avoiding duplicates.

---

## What it does

1. **Reads configuration** from `config.json` (symbols, download directory, DB credentials).
2. **Opens BVB pages** with Playwright in a stealth browser context (anti-bot tweaks).
3. For each symbol in `symbols_to_fetch`:

   * Navigates to `https://www.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=<SYMBOL>`
   * Clicks **“Tranzactionare”** → **“CSV”** to download the trading history
   * Saves the file to your configured `download_directory` as `<SYMBOL>_trading_history.csv`
4. **Creates** (if not present) a PostgreSQL table: `istoric_tranzactionare`
5. **Parses** the CSV and **inserts** rows (one per trading day), **skipping existing** `(data, simbol)` pairs
6. **Deletes** the CSV after successful import

Idempotency is ensured by checking for an existing row per `(data, simbol)` before insertion.

---

## Prerequisites

* Python 3.10+
* PostgreSQL 12+ (local or remote)
* Chrome dependencies (Playwright will install its own browser build)
* macOS / Linux / WSL recommended

Install Python deps:

```bash
python -m venv .venv && source .venv/bin/activate   # on Windows: .venv\Scripts\activate
pip install playwright psycopg2-binary
playwright install chromium
```

> If you run inside Docker, remember to install system packages required by Playwright (fonts, libnss, etc.).

---

## Configuration

Create `config.json` in the project root:

```json
{
  "symbols": ["ARS","SCD","EL","EBS","DIGI","BRD","PBK","ALU","CBC","RPH","SIF4","ATB","SAFE","AROBS","TLV","ALR","H2O","TRANSI","TEL","COTE","PE","INFINITY","RRC","AAG","BRK","SMTL","BNET","ROCE","ONE","TRP","SNG","TGN","SOCP","TTS","FP","TBM","M","EVER","SFG","UCM","RMAH","BIO","SNP","AQ","WINE","MECE","SNN","CRC","LION","IMP","TBK","BVB","CMF"],
  "download_directory": "/Users/laur/Downloads",
  "symbols_to_fetch": ["H2O","TLV","SNG","ONE","DIGI","SNP"],
  "database": {
    "dbname": "market_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
  }
}
```

### Environment variable overrides

You can override DB settings via env vars:

* `DB_NAME`, `DB_USER`, `DB_PASS`, `DB_HOST`, `DB_PORT`

---

## Usage

Run the script:

```bash
python bvb_download_ingest.py
```

What you’ll see:

* Progress logs per symbol
* Download confirmation to your `download_directory`
* Insert/skip messages per trading date
* CSV is removed after ingestion

---

## Database schema

On first run, the script ensures this table exists:

```sql
CREATE TABLE IF NOT EXISTS istoric_tranzactionare (
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

**Recommended for robustness**: enforce uniqueness and indexing for idempotency/performance:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS ux_istoric_data_symbol
  ON istoric_tranzactionare (data, simbol);

-- Or as a constraint:
-- ALTER TABLE istoric_tranzactionare
--   ADD CONSTRAINT uq_data_symbol UNIQUE (data, simbol);
```

### CSV columns expected (as downloaded from BVB “Tranzactionare” → “CSV”)

| Column (CSV)    | DB column        | Notes                                     |
| --------------- | ---------------- | ----------------------------------------- |
| Data            | data             | DD.MM.YYYY → converted to YYYY-MM-DD      |
| Piață           | piata            | Text                                      |
| Tranzacții      | tranzactii       | Integer (normalized: dots/commas removed) |
| Volum           | volum            | Integer (normalized)                      |
| Valoare         | valoare          | Decimal (`,` → `.`)                       |
| Preț deschidere | pret\_deschidere | Decimal                                   |
| Preț minim      | pret\_minim      | Decimal                                   |
| Preț maxim      | pret\_maxim      | Decimal                                   |
| Preț mediu      | pret\_mediu      | Decimal                                   |
| Preț închidere  | pret\_inchidere  | Decimal                                   |
| Variație (%)    | variatie         | Decimal (as number, not “%” text)         |
| —               | simbol           | Filled from the requested ticker          |

> The script cleans Romanian number formats: removes thousands separators (`.`), converts decimal commas to dots.

---

## How it works (flow)

1. **Config load** → `config.json`
2. **DB connect** → create table if missing
3. **For each `symbols_to_fetch`:**

   * Launch **Playwright** (Chromium, headless, stealth context)
   * Open symbol page → click **Tranzactionare** → click **CSV**
   * Save as `<SYMBOL>_trading_history.csv` to `download_directory`
   * Parse CSV → for each row:

     * Convert the date
     * **Skip** if `(data, simbol)` already in DB
     * Otherwise **insert**
   * Delete CSV
4. Close DB connection

---

## Troubleshooting

* **Playwright not installed**

  * Run:

    ```bash
    pip install playwright
    playwright install chromium
    ```
* **Missing system packages / fonts**

  * On Debian/Ubuntu, ensure common libraries are present (e.g., `libnss3`, `libatk1.0-0`, `libx11-xcb1`, `libxcomposite1`, `libxdamage1`, `libxrandr2`, `libgbm1`, `libpangocairo-1.0-0`, `libasound2`, `libxshmfence1`, fonts).
* **BVB page changed selectors**

  * The script targets:

    * `input[type="submit"][value="Tranzactionare"].btn.btnd`
    * `button:has-text("CSV")`
  * If BVB updates the site, adjust these selectors.
* **Duplicate rows**

  * Add the **unique index** above to enforce DB-level idempotency.
* **Permissions in `download_directory`**

  * Ensure the path exists and is writable by the user running the script.

---

## Notes & Extensions

* **Parallel downloads**: The script currently runs sequentially per symbol for simplicity. You can parallelize with `asyncio.gather()` if BVB allows it and you respect rate limiting.
* **Proxies / region**: If access is rate-limited, consider Playwright context proxies.
* **Dockerization**: Add a Dockerfile with Playwright’s base image to run consistently across environments.
* **Scheduling**: Use `cron`/systemd to run daily after market close.

---

## Security

* Prefer environment variables for DB credentials in CI/CD.
* If you commit `config.json`, **omit passwords** or use a `.env` file and `python-dotenv` (not required by this script, but recommended).

---

## License

Use and modify freely within your organization/project. Add a license file if you plan to distribute.
