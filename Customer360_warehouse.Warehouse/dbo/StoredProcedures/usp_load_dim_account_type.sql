-- ============================================================
-- 5. LOAD DIM_ACCOUNT_TYPE
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_account_type
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_account_type AS tgt
    USING
    (
        SELECT DISTINCT
            account_type,
            account_category,
            product_family,
            deposit_credit_type,
            interest_bearing_flag,
            risk_category,
            currency_code,
            is_active_product
        FROM Customer360_Lakehouse.dbo.silver_accounts
        WHERE data_quality_status NOT LIKE 'Invalid%'
          AND account_type IS NOT NULL
    ) AS src
    ON tgt.account_type = src.account_type

    WHEN MATCHED THEN
        UPDATE SET
            tgt.account_category = src.account_category,
            tgt.product_family = src.product_family,
            tgt.deposit_credit_type = src.deposit_credit_type,
            tgt.interest_bearing_flag = src.interest_bearing_flag,
            tgt.risk_category = src.risk_category,
            tgt.currency_code = src.currency_code,
            tgt.is_active_product = src.is_active_product

    WHEN NOT MATCHED THEN
        INSERT
        (
            account_type,
            account_category,
            product_family,
            deposit_credit_type,
            interest_bearing_flag,
            risk_category,
            currency_code,
            is_active_product
        )
        VALUES
        (
            src.account_type,
            src.account_category,
            src.product_family,
            src.deposit_credit_type,
            src.interest_bearing_flag,
            src.risk_category,
            src.currency_code,
            src.is_active_product
        );
END;