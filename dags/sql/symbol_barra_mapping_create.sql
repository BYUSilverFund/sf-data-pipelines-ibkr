CREATE TABLE IF NOT EXISTS symbol_barra_mapping (
    symbol TEXT PRIMARY KEY,
    barrid TEXT NOT NULL,
    first_detected_date DATE NOT NULL
);
