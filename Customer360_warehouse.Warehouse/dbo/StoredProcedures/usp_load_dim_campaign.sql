-- ============================================================
-- 7. LOAD DIM_CAMPAIGN
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_campaign
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_campaign AS tgt
    USING
    (
        SELECT DISTINCT
            campaign,
            channel,
            interaction_type,
            digital_channel_flag
        FROM Customer360_Lakehouse.dbo.silver_marketing_interactions
        WHERE data_quality_status NOT LIKE 'Invalid%'
    ) AS src
    ON ISNULL(tgt.campaign, '') = ISNULL(src.campaign, '')
   AND ISNULL(tgt.channel, '') = ISNULL(src.channel, '')
   AND ISNULL(tgt.interaction_type, '') = ISNULL(src.interaction_type, '')

    WHEN MATCHED THEN
        UPDATE SET
            tgt.digital_channel_flag = src.digital_channel_flag

    WHEN NOT MATCHED THEN
        INSERT
        (
            campaign,
            channel,
            interaction_type,
            digital_channel_flag
        )
        VALUES
        (
            src.campaign,
            src.channel,
            src.interaction_type,
            src.digital_channel_flag
        );
END;