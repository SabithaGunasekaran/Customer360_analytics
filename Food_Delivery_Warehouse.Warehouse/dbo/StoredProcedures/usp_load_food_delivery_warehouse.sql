CREATE       PROCEDURE dbo.usp_load_food_delivery_warehouse
AS
BEGIN

    SET NOCOUNT ON;


    /* ========================================================
       1. LOAD DIM_CUSTOMER
       ======================================================== */

    MERGE dbo.dim_customer AS tgt

USING
(
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        city,
        province,
        postal_code,
        signup_date,
        signup_year,
        preferred_cuisine,
        loyalty_member,
        marketing_opt_in

    FROM Food_Delivery_Lakehouse.dbo.silver_customers

) AS src

ON tgt.customer_id = src.customer_id


WHEN MATCHED THEN

    UPDATE SET

        tgt.customer_name = src.customer_name,
        tgt.email = src.email,
        tgt.phone = src.phone,

        tgt.city = src.city,
        tgt.province = src.province,
        tgt.postal_code = src.postal_code,

        tgt.signup_date = src.signup_date,
        tgt.signup_year = src.signup_year,

        tgt.preferred_cuisine = src.preferred_cuisine,
        tgt.loyalty_member = src.loyalty_member,
        tgt.marketing_opt_in = src.marketing_opt_in


WHEN NOT MATCHED THEN

    INSERT
    (
        customer_id,
        customer_name,
        email,
        phone,

        city,
        province,
        postal_code,

        signup_date,
        signup_year,

        preferred_cuisine,
        loyalty_member,
        marketing_opt_in
    )

    VALUES
    (
        src.customer_id,
        src.customer_name,
        src.email,
        src.phone,

        src.city,
        src.province,
        src.postal_code,

        src.signup_date,
        src.signup_year,

        src.preferred_cuisine,
        src.loyalty_member,
        src.marketing_opt_in
    );


    /* ========================================================
       2. LOAD DIM_RESTAURANT
       ======================================================== */

    MERGE dbo.dim_restaurant AS tgt

    USING
    (
        SELECT
            restaurant_id,
            restaurant_name,
            cuisine_type,
            city,
            restaurant_rating

        FROM Food_Delivery_Lakehouse.dbo.silver_restaurants

    ) AS src

    ON tgt.restaurant_id = src.restaurant_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.restaurant_name = src.restaurant_name,
            tgt.cuisine_type = src.cuisine_type,
            tgt.city = src.city,
            tgt.restaurant_rating = src.restaurant_rating

    WHEN NOT MATCHED THEN
        INSERT
        (
            restaurant_id,
            restaurant_name,
            cuisine_type,
            city,
            restaurant_rating
        )
        VALUES
        (
            src.restaurant_id,
            src.restaurant_name,
            src.cuisine_type,
            src.city,
            src.restaurant_rating
        );


    /* ========================================================
       3. LOAD FACT_ORDERS
       ======================================================== */

    MERGE dbo.fact_orders AS tgt

    USING
    (
        SELECT
            o.order_id,

            dc.customer_key,
            dr.restaurant_key,
            dd.date_key AS order_date_key,

            o.quantity,
            o.unit_price,
            o.order_amount,

            d.delivery_minutes,
            d.driver_rating,
            d.delivery_status,

            o.payment_method,
            o.order_status

        FROM Food_Delivery_Lakehouse.dbo.silver_orders o

        INNER JOIN dbo.dim_customer dc
            ON o.customer_id = dc.customer_id

        INNER JOIN dbo.dim_restaurant dr
            ON o.restaurant_id = dr.restaurant_id

        INNER JOIN dbo.dim_date dd
            ON o.order_date = dd.date_value

        LEFT JOIN Food_Delivery_Lakehouse.dbo.silver_deliveries d
            ON o.order_id = d.order_id

    ) AS src

    ON tgt.order_id = src.order_id

    WHEN MATCHED THEN
        UPDATE SET
            tgt.customer_key = src.customer_key,
            tgt.restaurant_key = src.restaurant_key,
            tgt.order_date_key = src.order_date_key,

            tgt.quantity = src.quantity,
            tgt.unit_price = src.unit_price,
            tgt.order_amount = src.order_amount,

            tgt.delivery_minutes = src.delivery_minutes,
            tgt.driver_rating = src.driver_rating,
            tgt.delivery_status = src.delivery_status,

            tgt.payment_method = src.payment_method,
            tgt.order_status = src.order_status

    WHEN NOT MATCHED THEN
        INSERT
        (
            order_id,
            customer_key,
            restaurant_key,
            order_date_key,

            quantity,
            unit_price,
            order_amount,

            delivery_minutes,
            driver_rating,
            delivery_status,

            payment_method,
            order_status
        )
        VALUES
        (
            src.order_id,
            src.customer_key,
            src.restaurant_key,
            src.order_date_key,

            src.quantity,
            src.unit_price,
            src.order_amount,

            src.delivery_minutes,
            src.driver_rating,
            src.delivery_status,

            src.payment_method,
            src.order_status
        );


    PRINT 'Food Delivery Warehouse load completed successfully.';

END;