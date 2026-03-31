
-- 1. Clear table
TRUNCATE TABLE idoc_quarterly_demographics;

-- 2. Update
INSERT INTO idoc_quarterly_demographics (
    race, sex, stnccty, hofnscd, age_decade, quarter_start, total_count
)
SELECT 
    race, sex, stnccty, hofnscd,
    FLOOR(EXTRACT(YEAR FROM AGE(record_date, birthdt)) / 10) * 10 AS age_decade,
    DATE_TRUNC('quarter', record_date) AS quarter_start, 
    COUNT(*) AS total_count
FROM 
    idoc_public_population
GROUP BY 
    race, sex, stnccty, hofnscd, age_decade, quarter_start;