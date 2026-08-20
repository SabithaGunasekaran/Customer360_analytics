CREATE   PROCEDURE dbo.usp_load_dim_date
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_date DATE = '2020-01-01';
    DECLARE @end_date   DATE = '2030-12-31';

    INSERT INTO dbo.dim_date
    (
        date_key,
        date_value,
        year,
        quarter_number,
        month_number,
        month_name,
        day_of_month,
        day_of_week,
        day_name,
        week_number,
        is_weekend
    )
    SELECT
        CAST(
            YEAR(DATEADD(DAY, value, @start_date)) * 10000
            + MONTH(DATEADD(DAY, value, @start_date)) * 100
            + DAY(DATEADD(DAY, value, @start_date))
            AS INT
        ) AS date_key,

        DATEADD(DAY, value, @start_date) AS date_value,

        YEAR(
            DATEADD(DAY, value, @start_date)
        ) AS year,

        DATEPART(
            QUARTER,
            DATEADD(DAY, value, @start_date)
        ) AS quarter_number,

        MONTH(
            DATEADD(DAY, value, @start_date)
        ) AS month_number,

        DATENAME(
            MONTH,
            DATEADD(DAY, value, @start_date)
        ) AS month_name,

        DAY(
            DATEADD(DAY, value, @start_date)
        ) AS day_of_month,

        DATEPART(
            WEEKDAY,
            DATEADD(DAY, value, @start_date)
        ) AS day_of_week,

        DATENAME(
            WEEKDAY,
            DATEADD(DAY, value, @start_date)
        ) AS day_name,

        DATEPART(
            WEEK,
            DATEADD(DAY, value, @start_date)
        ) AS week_number,

        CASE
            WHEN DATENAME(
                WEEKDAY,
                DATEADD(DAY, value, @start_date)
            ) IN ('Saturday', 'Sunday')
            THEN 1
            ELSE 0
        END AS is_weekend

    FROM GENERATE_SERIES
    (
        0,
        DATEDIFF(DAY, @start_date, @end_date),
        1
    )

    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.dim_date d
        WHERE d.date_value =
              DATEADD(DAY, value, @start_date)
    );

END;