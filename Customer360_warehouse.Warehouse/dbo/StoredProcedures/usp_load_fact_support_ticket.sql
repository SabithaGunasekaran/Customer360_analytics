-- ============================================================
-- 9. LOAD FACT_SUPPORT_TICKET
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_fact_support_ticket
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.fact_support_ticket AS tgt

    USING
    (
        SELECT
            st.ticket_id,
            dc.customer_key,
            dsc.support_category_key,
            dt.date_key AS ticket_date_key,
            dr.date_key AS resolution_date_key,
            st.resolution_days,
            st.ticket_age_days,
            st.sla_breached_flag,
            st.is_open_ticket

        FROM Customer360_Lakehouse.dbo.silver_support_tickets st

        INNER JOIN dbo.dim_customer dc
            ON st.customer_id = dc.customer_id
           AND dc.is_current = 1

        INNER JOIN dbo.dim_support_category dsc
            ON ISNULL(st.category, '') = ISNULL(dsc.category, '')
           AND ISNULL(st.priority, '') = ISNULL(dsc.priority, '')
           AND ISNULL(st.sla_target_days, -1) = ISNULL(dsc.sla_target_days, -1)

        LEFT JOIN dbo.dim_date dt
            ON st.ticket_date = dt.date_value

        LEFT JOIN dbo.dim_date dr
            ON st.resolution_date = dr.date_value

        WHERE st.data_quality_status NOT LIKE 'Invalid%'
    ) AS src

    ON tgt.ticket_id = src.ticket_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_key = src.customer_key,
            tgt.support_category_key = src.support_category_key,
            tgt.ticket_date_key = src.ticket_date_key,
            tgt.resolution_date_key = src.resolution_date_key,
            tgt.resolution_days = src.resolution_days,
            tgt.ticket_age_days = src.ticket_age_days,
            tgt.sla_breached_flag = src.sla_breached_flag,
            tgt.is_open_ticket = src.is_open_ticket

    WHEN NOT MATCHED THEN
        INSERT
        (
            ticket_id,
            customer_key,
            support_category_key,
            ticket_date_key,
            resolution_date_key,
            resolution_days,
            ticket_age_days,
            sla_breached_flag,
            is_open_ticket
        )
        VALUES
        (
            src.ticket_id,
            src.customer_key,
            src.support_category_key,
            src.ticket_date_key,
            src.resolution_date_key,
            src.resolution_days,
            src.ticket_age_days,
            src.sla_breached_flag,
            src.is_open_ticket
        );
END;