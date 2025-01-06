from common.constants import Redshift

SCHEMA = Redshift.SchemaName.SILVER
CALENDAR_TABLE_NAME = Redshift.TableName.DIM_CALENDAR
CALENDAR_PROCEDURE_NAME = "generate_dim_calendar_of_year"


CREATE_CALENDAR_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{CALENDAR_TABLE_NAME} (
        date DATE PRIMARY KEY,
        year INTEGER NOT NULL,
        quarter INTEGER NOT NULL,
        quarter_id VARCHAR(8) NOT NULL,
        month_num INTEGER NOT NULL,
        month_id VARCHAR(6) NOT NULL,
        month_name VARCHAR(3) NOT NULL,
        day_of_month INTEGER NOT NULL,
        day_of_week INTEGER NOT NULL,
        day_name VARCHAR(10) NOT NULL,
        is_market_holiday BOOLEAN NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
"""

CREATE_CALENDAR_PRECEDURE_SQL = f"""
CREATE OR REPLACE PROCEDURE {SCHEMA}.{CALENDAR_PROCEDURE_NAME}(input_year INTEGER)
AS $$
DECLARE
    start_date DATE := (CAST(input_year || '-01-01' AS DATE));
    end_date DATE := (CAST(input_year || '-12-31' AS DATE));
    current_date DATE;
    calendar_count INTEGER;
begin
    -- full refresh를 위한 데이터 삭제
	DELETE FROM {SCHEMA}.{CALENDAR_TABLE_NAME} WHERE year=input_year;

	current_date := start_date;
    WHILE current_date <= end_date LOOP
        BEGIN
            INSERT INTO {SCHEMA}.{CALENDAR_TABLE_NAME} (
                date, 
                year, 
                quarter, 
                quarter_id, 
                month_num, 
                month_id, 
                month_name, 
                day_of_month, 
                day_of_week, 
                day_name, 
                is_market_holiday
            ) VALUES (
                current_date,
                EXTRACT(YEAR FROM current_date),
                EXTRACT(QUARTER FROM current_date),
                EXTRACT(YEAR FROM current_date) || 'Q' || EXTRACT(QUARTER FROM current_date),
                EXTRACT(MONTH FROM current_date),
                TO_CHAR(current_date, 'YYYYMM'),
                TO_CHAR(current_date, 'Mon'),
                EXTRACT(DAY FROM current_date),
                EXTRACT(DOW FROM current_date),
                CASE
                    WHEN EXTRACT(DOW FROM current_date) = 0 THEN '일요일'
                    WHEN EXTRACT(DOW FROM current_date) = 1 THEN '월요일'
                    WHEN EXTRACT(DOW FROM current_date) = 2 THEN '화요일'
                    WHEN EXTRACT(DOW FROM current_date) = 3 THEN '수요일'
                    WHEN EXTRACT(DOW FROM current_date) = 4 THEN '목요일'
                    WHEN EXTRACT(DOW FROM current_date) = 5 THEN '금요일'
                    ELSE '토요일'
                END,
                CASE
                    WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE
                    ELSE FALSE
                END
            );
        END;
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
end;
$$ LANGUAGE plpgsql;
"""

CALL_CALENDAR_PROCEDURE_SQL = (
    f"CALL {SCHEMA}.{CALENDAR_PROCEDURE_NAME}(:calendar_year);"
)
