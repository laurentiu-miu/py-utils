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
