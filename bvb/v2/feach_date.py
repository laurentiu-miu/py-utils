import asyncio
import os
import random
import json
import csv
from datetime import datetime
from pathlib import Path
import psycopg2

from psycopg2.extras import execute_values
from playwright.async_api import async_playwright

# Load configuration
with open('config.json') as config_file:
    config = json.load(config_file)

db_config = config["database"]
symbols_to_fetch = config["symbols_to_fetch"]
download_dir = Path(config["download_directory"])

db_name = os.getenv("DB_NAME", db_config.get("dbname"))
db_user = os.getenv("DB_USER", db_config.get("user"))
db_pass = os.getenv("DB_PASS", db_config.get("password"))
db_host = os.getenv("DB_HOST", db_config.get("host"))
db_port = int(os.getenv("DB_PORT", db_config.get("port", 5432)))

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_pass,
    host=db_host,
    port=db_port
)
cur = conn.cursor()
cur.execute("""
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
""")
conn.commit()


async def create_stealth_browser(p):
    """Create a browser with stealth settings to avoid detection"""
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15"
    ]
    browser_context_options = {
        'user_agent': random.choice(user_agents),
        'viewport': {'width': 1920, 'height': 1080},
        'device_scale_factor': 1,
        'is_mobile': False,
        'has_touch': False,
        'locale': 'en-US',
        'timezone_id': 'America/New_York'
    }
    browser = await p.chromium.launch(headless=True, args=["--headless=new"])
    context = await browser.new_context(**browser_context_options)

    # Anti-bot patches
    await context.add_init_script("""Object.defineProperty(navigator, 'webdriver', { get: () => undefined });""")
    await context.add_init_script("""window.chrome = { runtime: {} };""")
    await context.add_init_script("""
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
        );
    """)
    await context.add_init_script("""
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    """)
    return browser, context


async def download_symbol_csv(symbol):
    async with async_playwright() as p:
        browser, context = await create_stealth_browser(p)
        page = await context.new_page()
        download_dir = Path.cwd() / "downloads"
        download_dir.mkdir(exist_ok=True)

        try:
            print(f"Navigating to page for {symbol}...")
            url = f"https://www.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s={symbol}"
            await page.goto(url, wait_until="networkidle")

            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)

            # Click "Tranzactionare"
            await page.locator('input[type="submit"][value="Tranzactionare"].btn.btnd').click()
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)

            # Download CSV
            async with page.expect_download() as download_info:
                csv_button = page.locator('button:has-text("CSV")')
                await csv_button.wait_for(state="visible", timeout=10000)
                await csv_button.click()

            download = await download_info.value
            filename = f"{symbol}_trading_history.csv"
            filepath = download_dir / filename
            await download.save_as(filepath)
            print(f"✓ Downloaded {symbol} CSV to {filepath}")
            return filepath

        except Exception as e:
            print(f"❌ Error for {symbol}: {e}")
            return None
        finally:
            await browser.close()

def _parse_int(s: str) -> int:
    s = (s or "").strip().replace('.', '').replace(',', '')
    return int(s) if s else 0

def _parse_float(s: str):
    s = (s or "").strip()
    # elimină separatoare de mii și convertește la punct zecimal
    s = s.replace('.', '').replace(',', '.')
    return float(s) if s else None

def insert_csv_to_db(filepath, simbol):
    # 1) Ultima dată deja încărcată pentru simbol
    cur.execute("SELECT MAX(data) FROM istoric_tranzactionare WHERE simbol = %s", (simbol,))
    last_date = cur.fetchone()[0]  # poate fi None
    # 2) Citește CSV și păstrează doar rândurile cu data > last_date
    rows_to_insert = []
    with open(filepath, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # skip header dacă există
        for row in reader:
            # row[0] = dd.MM.yyyy
            data_date = datetime.strptime(row[0], '%d.%m.%Y').date()
            if last_date and data_date <= last_date:
                continue

            rows_to_insert.append((
                data_date,                  # data
                row[1],                     # piata
                _parse_int(row[2]),         # tranzactii
                _parse_int(row[3]),         # volum
                _parse_float(row[4]),       # valoare
                _parse_float(row[5]),       # pret_deschidere
                _parse_float(row[6]),       # pret_minim
                _parse_float(row[7]),       # pret_maxim
                _parse_float(row[8]),       # pret_mediu
                _parse_float(row[9]),       # pret_inchidere
                _parse_float(row[10]),      # variatie
                simbol
            ))

    if not rows_to_insert:
        print(f"• Nimic nou de adăugat pentru {simbol}" +
              (f" după {last_date.isoformat()}." if last_date else "."))
        os.remove(filepath)
        return

    # opțional: păstrează ordinea cronologică
    rows_to_insert.sort(key=lambda r: r[0])

    # 3) Bulk insert (o singură interogare)
    execute_values(cur, """
        INSERT INTO istoric_tranzactionare
        (data, piata, tranzactii, volum, valoare, pret_deschidere, pret_minim,
         pret_maxim, pret_mediu, pret_inchidere, variatie, simbol)
        VALUES %s
    """, rows_to_insert, page_size=1000)

    conn.commit()
    os.remove(filepath)
    print(f"✓ Inserate {len(rows_to_insert)} rânduri pentru {simbol} și șters fișierul.")


async def main():
    for symbol in symbols_to_fetch:
        filepath = await download_symbol_csv(symbol)
        if filepath and filepath.exists():
            insert_csv_to_db(filepath, symbol)


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    conn.close()
