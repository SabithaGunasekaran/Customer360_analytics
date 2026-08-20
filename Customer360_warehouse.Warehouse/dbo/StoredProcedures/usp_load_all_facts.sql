-- ============================================================
-- 12. MASTER FACT LOAD
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_all_facts
AS
BEGIN
    SET NOCOUNT ON;

    EXEC dbo.usp_load_fact_account_snapshot;
    EXEC dbo.usp_load_fact_support_ticket;
    EXEC dbo.usp_load_fact_marketing_interaction;
END;