-- ============================================================
-- 4. LOAD DIM_BRANCH
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_branch
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_branch AS tgt
    USING
    (
        SELECT DISTINCT
            branch_code,
            branch_name,
            branch_city,
            branch_state,
            branch_country,
            branch_region,
            branch_type,
            is_active_branch
        FROM Customer360_Lakehouse.dbo.silver_accounts
        WHERE data_quality_status NOT LIKE 'Invalid%'
          AND branch_code IS NOT NULL
    ) AS src
    ON tgt.branch_code = src.branch_code

    WHEN MATCHED THEN
        UPDATE SET
            tgt.branch_name = src.branch_name,
            tgt.city = src.branch_city,
            tgt.state = src.branch_state,
            tgt.country = src.branch_country,
            tgt.region = src.branch_region,
            tgt.branch_type = src.branch_type,
            tgt.is_active_branch = src.is_active_branch

    WHEN NOT MATCHED THEN
        INSERT
        (
            branch_code,
            branch_name,
            city,
            state,
            country,
            region,
            branch_type,
            is_active_branch
        )
        VALUES
        (
            src.branch_code,
            src.branch_name,
            src.branch_city,
            src.branch_state,
            src.branch_country,
            src.branch_region,
            src.branch_type,
            src.is_active_branch
        );
END;