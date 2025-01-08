-- TODO: 스키마명, 테이블 이름을 동적으로 받아오기 위한 처리가 필요합니다.

-- dim_stat_code 테이블 없는 경우 생성
CREATE TABLE IF NOT EXISTS silver.dim_stat_code (
    stat_code VARCHAR(20) NOT NULL,
    stat_name VARCHAR(100) NOT NULL,
    unit_name VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (stat_code)
);

-- fact_economic_indicators 테이블 생성
CREATE TABLE IF NOT EXISTS silver.fact_economic_indicators (
    time VARCHAR(20) NOT NULL,
    stat_code VARCHAR(20) NOT NULL,
    country_code VARCHAR(20) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    date DATE NOT NULL,
    frequency VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT GETDATE(),
    updated_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (time, stat_code, country_code)
) DISTKEY (stat_code) SORTKEY (date);
