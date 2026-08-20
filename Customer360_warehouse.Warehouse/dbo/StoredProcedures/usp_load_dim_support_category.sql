-- ============================================================
-- 6. LOAD DIM_SUPPORT_CATEGORY
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_support_category
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_support_category AS tgt
    USING
    (
        SELECT DISTINCT
            category,
            priority,
            sla_target_days
        FROM Customer360_Lakehouse.dbo.silver_support_tickets
        WHERE data_quality_status NOT LIKE 'Invalid%'
    ) AS src
    ON ISNULL(tgt.category, '') = ISNULL(src.category, '')
   AND ISNULL(tgt.priority, '') = ISNULL(src.priority, '')
   AND ISNULL(tgt.sla_target_days, -1) = ISNULL(src.sla_target_days, -1)

    WHEN NOT MATCHED THEN
        INSERT
        (
            category,
            priority,
            sla_target_days
        )
        VALUES
        (
            src.category,
            src.priority,
            src.sla_target_days
        );
END;