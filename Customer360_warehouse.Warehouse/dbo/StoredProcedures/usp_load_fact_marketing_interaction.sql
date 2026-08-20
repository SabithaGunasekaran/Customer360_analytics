-- ============================================================
-- 10. LOAD FACT_MARKETING_INTERACTION
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_fact_marketing_interaction
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.fact_marketing_interaction AS tgt

    USING
    (
        SELECT
            mi.interaction_id,
            dc.customer_key,
            dcamp.campaign_key,
            dd.date_key AS interaction_date_key,
            mi.responded_flag,
            mi.days_since_interaction

        FROM Customer360_Lakehouse.dbo.silver_marketing_interactions mi

        INNER JOIN dbo.dim_customer dc
            ON mi.customer_id = dc.customer_id
           AND dc.is_current = 1

        INNER JOIN dbo.dim_campaign dcamp
            ON ISNULL(mi.campaign, '') = ISNULL(dcamp.campaign, '')
           AND ISNULL(mi.channel, '') = ISNULL(dcamp.channel, '')
           AND ISNULL(mi.interaction_type, '') = ISNULL(dcamp.interaction_type, '')

        LEFT JOIN dbo.dim_date dd
            ON mi.interaction_date = dd.date_value

        WHERE mi.data_quality_status NOT LIKE 'Invalid%'
    ) AS src

    ON tgt.interaction_id = src.interaction_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_key = src.customer_key,
            tgt.campaign_key = src.campaign_key,
            tgt.interaction_date_key = src.interaction_date_key,
            tgt.responded_flag = src.responded_flag,
            tgt.days_since_interaction = src.days_since_interaction

    WHEN NOT MATCHED THEN
        INSERT
        (
            interaction_id,
            customer_key,
            campaign_key,
            interaction_date_key,
            responded_flag,
            days_since_interaction
        )
        VALUES
        (
            src.interaction_id,
            src.customer_key,
            src.campaign_key,
            src.interaction_date_key,
            src.responded_flag,
            src.days_since_interaction
        );
END;