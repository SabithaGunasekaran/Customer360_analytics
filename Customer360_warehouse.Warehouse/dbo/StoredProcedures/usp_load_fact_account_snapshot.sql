-- ============================================================
-- 8. LOAD FACT_ACCOUNT_SNAPSHOT
-- Grain: one row per account per snapshot date
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_fact_account_snapshot
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @snapshot_date DATE = CAST(GETDATE() AS DATE);

    INSERT INTO dbo.fact_account_snapshot
    (
        customer_key,
        account_key,
        snapshot_date_key,
        branch_key,
        account_type_key,
        balance,
        is_active_account
    )
    SELECT
        dc.customer_key,
        da.account_key,
        dd.date_key,
        db.branch_key,
        dat.account_type_key,
        sa.balance,
        sa.is_active_account
    FROM Customer360_Lakehouse.dbo.silver_accounts sa

    INNER JOIN dbo.dim_customer dc
        ON sa.customer_id = dc.customer_id
       AND dc.is_current = 1

    INNER JOIN dbo.dim_account da
        ON sa.account_id = da.account_id

    INNER JOIN dbo.dim_branch db
        ON sa.branch_code = db.branch_code

    INNER JOIN dbo.dim_account_type dat
        ON sa.account_type = dat.account_type

    INNER JOIN dbo.dim_date dd
        ON dd.date_value = @snapshot_date

    WHERE sa.data_quality_status NOT LIKE 'Invalid%'

      AND NOT EXISTS
      (
          SELECT 1
          FROM dbo.fact_account_snapshot f
          WHERE f.account_key = da.account_key
            AND f.snapshot_date_key = dd.date_key
      );
END;