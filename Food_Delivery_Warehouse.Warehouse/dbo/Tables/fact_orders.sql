CREATE TABLE [dbo].[fact_orders] (

	[order_key] bigint IDENTITY NOT NULL, 
	[order_id] varchar(50) NOT NULL, 
	[customer_key] bigint NOT NULL, 
	[restaurant_key] bigint NOT NULL, 
	[order_date_key] int NOT NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(10,2) NULL, 
	[order_amount] decimal(12,2) NULL, 
	[delivery_minutes] int NULL, 
	[driver_rating] decimal(3,1) NULL, 
	[delivery_status] varchar(50) NULL, 
	[payment_method] varchar(50) NULL, 
	[order_status] varchar(50) NULL
);