CREATE   PROCEDURE dbo.usp_load_dim_date
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @date DATE = '2024-01-01';
    DECLARE @end_date DATE = '2030-12-31';

    WHILE @date <= @end_date
    BEGIN

        IF NOT EXISTS
        (
            SELECT 1
            FROM dbo.dim_date
            WHERE date_key =
                  YEAR(@date) * 10000
                + MONTH(@date) * 100
                + DAY(@date)
        )
        BEGIN

            INSERT INTO dbo.dim_date
            (
                date_key,
                date_value,
                year,
                quarter_number,
                month_number,
                month_name,
                day_of_month,
                day_name
            )
            VALUES
            (
                YEAR(@date) * 10000
                    + MONTH(@date) * 100
                    + DAY(@date),

                @date,
                YEAR(@date),
                DATEPART(QUARTER, @date),
                MONTH(@date),
                DATENAME(MONTH, @date),
                DAY(@date),
                DATENAME(WEEKDAY, @date)
            );

        END;

        SET @date = DATEADD(
            DAY,
            1,
            @date
        );

    END;

END;