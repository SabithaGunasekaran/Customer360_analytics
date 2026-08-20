-- ============================================================
-- 11. MASTER DIMENSION LOAD
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_all_dimensions
AS
BEGIN
    SET NOCOUNT ON;

    EXEC dbo.usp_load_dim_date;
    EXEC dbo.usp_load_dim_customer;
    EXEC dbo.usp_load_dim_account;
    EXEC dbo.usp_load_dim_branch;
    EXEC dbo.usp_load_dim_account_type;
    EXEC dbo.usp_load_dim_support_category;
    EXEC dbo.usp_load_dim_campaign;
END;