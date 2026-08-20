-- ============================================================
-- 3. LOAD DIM_ACCOUNT
-- SCD TYPE 1
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_account
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_account AS tgt
    USING
    (
        SELECT
            account_id,
            account_status,
            opened_date,
            closed_date,
            account_age_years,
            balance_band,
            is_active_account
        FROM Customer360_Lakehouse.dbo.silver_accounts
        WHERE data_quality_status NOT LIKE 'Invalid%'
    ) AS src
    ON tgt.account_id = src.account_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.account_status = src.account_status,
            tgt.opened_date = src.opened_date,
            tgt.closed_date = src.closed_date,
            tgt.account_age_years = src.account_age_years,
            tgt.balance_band = src.balance_band,
            tgt.is_active_account = src.is_active_account

    WHEN NOT MATCHED THEN
        INSERT
        (
            account_id,
            account_status,
            opened_date,
            closed_date,
            account_age_years,
            balance_band,
            is_active_account
        )
        VALUES
        (
            src.account_id,
            src.account_status,
            src.opened_date,
            src.closed_date,
            src.account_age_years,
            src.balance_band,
            src.is_active_account
        );
END;