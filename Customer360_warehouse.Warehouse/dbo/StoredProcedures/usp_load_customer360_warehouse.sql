-- ============================================================
-- 13. MASTER CUSTOMER 360 WAREHOUSE LOAD
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_customer360_warehouse
AS
BEGIN
    SET NOCOUNT ON;

    EXEC dbo.usp_load_all_dimensions;
    EXEC dbo.usp_load_all_facts;
END;