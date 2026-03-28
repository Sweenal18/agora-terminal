

-- Grain: one row per calendar date, 2015-01-01 through 2030-12-31.
-- date_key is YYYYMMDD integer (e.g. 20240315) -- human-readable AND fast to join.
-- Contains all date attributes needed for financial analysis so downstream
-- consumers never recompute them: trading day flags, fiscal periods,
-- relative flags (is_ytd, is_last_30_days, etc).

WITH date_spine AS (

    SELECT
        CAST(UNNEST(GENERATE_SERIES(
            DATE '2015-01-01',
            DATE '2030-12-31',
            INTERVAL '1 DAY'
        )) AS DATE) AS calendar_date

),

us_market_holidays AS (

    -- NYSE/NASDAQ market holidays (static rules).
    -- Excludes weekends and the 9 US federal market holidays.
    SELECT calendar_date FROM date_spine WHERE

        -- New Year's Day (Jan 1, observed)
        (MONTH(calendar_date) = 1  AND DAY(calendar_date) = 1  AND DAYOFWEEK(calendar_date) BETWEEN 1 AND 5)
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 31 AND DAYOFWEEK(calendar_date) = 5)
        OR (MONTH(calendar_date) = 1  AND DAY(calendar_date) = 2  AND DAYOFWEEK(calendar_date) = 1)

        -- MLK Day (3rd Monday of January)
        OR (MONTH(calendar_date) = 1  AND DAYOFWEEK(calendar_date) = 1
            AND DAY(calendar_date) BETWEEN 15 AND 21)

        -- Presidents Day (3rd Monday of February)
        OR (MONTH(calendar_date) = 2  AND DAYOFWEEK(calendar_date) = 1
            AND DAY(calendar_date) BETWEEN 15 AND 21)

        -- Memorial Day (last Monday of May)
        OR (MONTH(calendar_date) = 5  AND DAYOFWEEK(calendar_date) = 1
            AND DAY(calendar_date) BETWEEN 25 AND 31)

        -- Juneteenth (June 19, observed)
        OR (MONTH(calendar_date) = 6  AND DAY(calendar_date) = 19 AND DAYOFWEEK(calendar_date) BETWEEN 1 AND 5)
        OR (MONTH(calendar_date) = 6  AND DAY(calendar_date) = 20 AND DAYOFWEEK(calendar_date) = 1)
        OR (MONTH(calendar_date) = 6  AND DAY(calendar_date) = 18 AND DAYOFWEEK(calendar_date) = 5)

        -- Independence Day (July 4, observed)
        OR (MONTH(calendar_date) = 7  AND DAY(calendar_date) = 4  AND DAYOFWEEK(calendar_date) BETWEEN 1 AND 5)
        OR (MONTH(calendar_date) = 7  AND DAY(calendar_date) = 5  AND DAYOFWEEK(calendar_date) = 1)
        OR (MONTH(calendar_date) = 7  AND DAY(calendar_date) = 3  AND DAYOFWEEK(calendar_date) = 5)

        -- Labor Day (1st Monday of September)
        OR (MONTH(calendar_date) = 9  AND DAYOFWEEK(calendar_date) = 1
            AND DAY(calendar_date) BETWEEN 1 AND 7)

        -- Thanksgiving (4th Thursday of November)
        OR (MONTH(calendar_date) = 11 AND DAYOFWEEK(calendar_date) = 4
            AND DAY(calendar_date) BETWEEN 22 AND 28)

        -- Christmas (Dec 25, observed)
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 25 AND DAYOFWEEK(calendar_date) BETWEEN 1 AND 5)
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 26 AND DAYOFWEEK(calendar_date) = 1)
        OR (MONTH(calendar_date) = 12 AND DAY(calendar_date) = 24 AND DAYOFWEEK(calendar_date) = 5)

),

final AS (

    SELECT
        -- Surrogate key: YYYYMMDD integer -- sortable, human-readable, fast
        CAST(STRFTIME(d.calendar_date, '%Y%m%d') AS INTEGER)    AS date_key,

        -- Natural key
        d.calendar_date,

        -- Year and period
        YEAR(d.calendar_date)                                   AS year,
        QUARTER(d.calendar_date)                                AS quarter,
        MONTH(d.calendar_date)                                  AS month_number,
        STRFTIME(d.calendar_date, '%B')                         AS month_name,
        STRFTIME(d.calendar_date, '%b')                         AS month_name_short,
        WEEK(d.calendar_date)                                   AS week_of_year,
        DAYOFYEAR(d.calendar_date)                              AS day_of_year,

        -- Week
        DAYOFWEEK(d.calendar_date)                              AS day_of_week,
        STRFTIME(d.calendar_date, '%A')                         AS day_name,
        STRFTIME(d.calendar_date, '%a')                         AS day_name_short,

        -- Fiscal periods (calendar year convention)
        YEAR(d.calendar_date)                                   AS fiscal_year,
        QUARTER(d.calendar_date)                                AS fiscal_quarter,
        CONCAT('FY', YEAR(d.calendar_date))                     AS fiscal_year_label,
        CONCAT('FY', YEAR(d.calendar_date), ' Q', QUARTER(d.calendar_date)) AS fiscal_quarter_label,

        -- Human-readable labels
        STRFTIME(d.calendar_date, '%Y-%m')                      AS year_month,
        CONCAT('Q', QUARTER(d.calendar_date), ' ', YEAR(d.calendar_date)) AS quarter_label,

        -- Boolean flags
        DAYOFWEEK(d.calendar_date) NOT IN (0, 6)                AS is_weekday,
        DAYOFWEEK(d.calendar_date) IN (0, 6)                    AS is_weekend,
        (DAYOFWEEK(d.calendar_date) NOT IN (0, 6)
            AND h.calendar_date IS NULL)                        AS is_trading_day,
        h.calendar_date IS NOT NULL                             AS is_market_holiday,
        DAY(d.calendar_date) = 1                                AS is_month_start,
        d.calendar_date = LAST_DAY(d.calendar_date)             AS is_month_end,
        (MONTH(d.calendar_date) IN (1,4,7,10)
            AND DAY(d.calendar_date) = 1)                       AS is_quarter_start,

        -- Relative to today (computed at model run time)
        d.calendar_date = CURRENT_DATE                          AS is_today,
        (d.calendar_date >= CURRENT_DATE - INTERVAL '7 days'
            AND d.calendar_date < CURRENT_DATE)                 AS is_last_7_days,
        (d.calendar_date >= CURRENT_DATE - INTERVAL '30 days'
            AND d.calendar_date < CURRENT_DATE)                 AS is_last_30_days,
        (d.calendar_date >= CURRENT_DATE - INTERVAL '365 days'
            AND d.calendar_date < CURRENT_DATE)                 AS is_last_365_days,
        d.calendar_date >= DATE_TRUNC('year', CURRENT_DATE)     AS is_ytd

    FROM date_spine d
    LEFT JOIN us_market_holidays h ON d.calendar_date = h.calendar_date

)

SELECT * FROM final
ORDER BY calendar_date