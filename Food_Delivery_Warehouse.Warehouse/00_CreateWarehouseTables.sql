CREATE TABLE dbo.dim_customer
(
    customer_key       BIGINT IDENTITY NOT NULL,
    customer_id        VARCHAR(50) NOT NULL,

    customer_name      VARCHAR(150) NULL,
    email              VARCHAR(200) NULL,
    phone              VARCHAR(30) NULL,

    city               VARCHAR(100) NULL,
    province           VARCHAR(50) NULL,
    postal_code        VARCHAR(20) NULL,

    signup_date        DATE NULL,
    signup_year        INT NULL,

    preferred_cuisine  VARCHAR(100) NULL,
    loyalty_member     BIT NULL,
    marketing_opt_in   BIT NULL
);

CREATE TABLE dbo.dim_restaurant
(
    restaurant_key     BIGINT IDENTITY NOT NULL,
    restaurant_id      VARCHAR(50) NOT NULL,

    restaurant_name    VARCHAR(200) NULL,
    cuisine_type       VARCHAR(100) NULL,
    city               VARCHAR(100) NULL,
    restaurant_rating  DECIMAL(3,1) NULL
);

CREATE TABLE dbo.dim_date
(
    date_key          INT NOT NULL,
    date_value        DATE NOT NULL,

    year              INT NULL,
    quarter_number    INT NULL,

    month_number      INT NULL,
    month_name        VARCHAR(20) NULL,

    day_of_month      INT NULL,
    day_name          VARCHAR(20) NULL
);

CREATE TABLE dbo.fact_orders
(
    order_key          BIGINT IDENTITY NOT NULL,
    order_id           VARCHAR(50) NOT NULL,

    customer_key       BIGINT NOT NULL,
    restaurant_key     BIGINT NOT NULL,
    order_date_key     INT NOT NULL,

    quantity           INT NULL,
    unit_price         DECIMAL(10,2) NULL,
    order_amount       DECIMAL(12,2) NULL,

    delivery_minutes   INT NULL,
    driver_rating      DECIMAL(3,1) NULL,
    delivery_status    VARCHAR(50) NULL,

    payment_method     VARCHAR(50) NULL,
    order_status       VARCHAR(50) NULL
);

