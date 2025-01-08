SWAP_TABLE_QUERY = """
BEGIN;

DROP TABLE IF EXISTS silver.{target_table}_backup;

ALTER TABLE silver.{target_table}
    RENAME TO {target_table}_backup;

CREATE TABLE silver.{target_table} AS
    SELECT * FROM staging.{target_table};

COMMIT;
"""

INITIALIZE_QUERY = """
BEGIN;

DROP TABLE IF EXISTS staging.{target_table};
CREATE TABLE staging.{target_table} (
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (date, ticker)
)
DISTKEY (ticker) 
SORTKEY (date);

CREATE TABLE IF NOT EXISTS silver.{target_table} (
    date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (date, ticker)
)
DISTKEY (ticker) 
SORTKEY (date);

COMMIT;
"""
