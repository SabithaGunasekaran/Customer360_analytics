-- ============================================================
-- 2. LOAD DIM_CUSTOMER
-- SCD TYPE 2
-- ============================================================

CREATE   PROCEDURE dbo.usp_load_dim_customer
AS
BEGIN
    SET NOCOUNT ON;

    -- Expire changed current rows
    UPDATE tgt
    SET
        tgt.effective_to = GETDATE(),
        tgt.is_current = 0
    FROM dbo.dim_customer tgt
    INNER JOIN Customer360_Lakehouse.dbo.silver_customers src
        ON tgt.customer_id = src.customer_id
    WHERE tgt.is_current = 1
      AND src.data_quality_status IN ('Valid', 'Warning - Invalid or Missing Email')
      AND
      (
            ISNULL(tgt.first_name, '') <> ISNULL(src.first_name, '')
         OR ISNULL(tgt.last_name, '') <> ISNULL(src.last_name, '')
         OR ISNULL(tgt.full_name, '') <> ISNULL(src.full_name, '')
         OR ISNULL(tgt.email, '') <> ISNULL(src.email, '')
         OR ISNULL(tgt.phone, '') <> ISNULL(src.phone, '')
         OR ISNULL(tgt.gender, '') <> ISNULL(src.gender, '')
         OR ISNULL(tgt.city, '') <> ISNULL(src.city, '')
         OR ISNULL(tgt.state, '') <> ISNULL(src.state, '')
         OR ISNULL(tgt.country, '') <> ISNULL(src.country, '')
         OR ISNULL(tgt.customer_status, '') <> ISNULL(src.customer_status, '')
         OR ISNULL(tgt.is_active_customer, 0) <> ISNULL(src.is_active_customer, 0)
         OR ISNULL(tgt.customer_tenure_years, 0) <> ISNULL(src.customer_tenure_years, 0)
      );

    -- Insert new customers and new SCD2 versions
    INSERT INTO dbo.dim_customer
    (
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        gender,
        date_of_birth,
        age,
        age_group,
        city,
        state,
        country,
        customer_status,
        is_active_customer,
        customer_tenure_years,
        effective_from,
        effective_to,
        is_current
    )
    SELECT
        src.customer_id,
        src.first_name,
        src.last_name,
        src.full_name,
        src.email,
        src.phone,
        src.gender,
        src.date_of_birth,
        src.age,
        src.age_group,
        src.city,
        src.state,
        src.country,
        src.customer_status,
        src.is_active_customer,
        src.customer_tenure_years,
        GETDATE(),
        NULL,
        1
    FROM Customer360_Lakehouse.dbo.silver_customers src
    LEFT JOIN dbo.dim_customer tgt
        ON src.customer_id = tgt.customer_id
       AND tgt.is_current = 1
    WHERE tgt.customer_id IS NULL
      AND src.data_quality_status IN ('Valid', 'Warning - Invalid or Missing Email');
END;