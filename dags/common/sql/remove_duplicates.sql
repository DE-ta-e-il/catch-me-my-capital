-- TODO: 스키마명, 테이블 이름을 동적으로 받아오기 위한 처리가 필요합니다.
BEGIN;

-- dim_stat_code
DROP TABLE IF EXISTS silver.dim_stat_code_temp;

CREATE TABLE silver.dim_stat_code_temp
AS (
    SELECT
        stat_code,
        stat_name,
        unit_name,
        created_at,
        updated_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY stat_code
                ORDER BY created_at DESC
            ) AS rn
        FROM silver.dim_stat_code
    )
    WHERE rn = 1
);

DROP TABLE silver.dim_stat_code;

ALTER TABLE silver.dim_stat_code_temp
RENAME TO dim_stat_code;

-- fact_economic_indicators
DROP TABLE IF EXISTS silver.fact_economic_indicators_temp;

CREATE TABLE silver.fact_economic_indicators_temp
DISTKEY (stat_code) SORTKEY (date)
AS (
    SELECT
        (time),
        stat_code,
        country_code,
        value,
        date,
        frequency,
        created_at,
        updated_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY "time", stat_code, country_code
                ORDER BY created_at DESC
            ) AS rn
        FROM silver.fact_economic_indicators
    )
    WHERE rn = 1
);

DROP TABLE silver.fact_economic_indicators;

ALTER TABLE silver.fact_economic_indicators_temp
RENAME TO fact_economic_indicators;

COMMIT;
