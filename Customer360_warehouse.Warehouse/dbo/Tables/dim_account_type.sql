CREATE TABLE [dbo].[dim_account_type] (

	[account_type_key] bigint IDENTITY NOT NULL, 
	[account_type] varchar(50) NOT NULL, 
	[account_category] varchar(50) NULL, 
	[product_family] varchar(100) NULL, 
	[deposit_credit_type] varchar(20) NULL, 
	[interest_bearing_flag] bit NULL, 
	[risk_category] varchar(50) NULL, 
	[currency_code] varchar(10) NULL, 
	[is_active_product] bit NULL
);