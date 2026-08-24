CREATE TABLE [dbo].[dim_customer] (

	[customer_key] bigint IDENTITY NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[customer_name] varchar(150) NULL, 
	[email] varchar(200) NULL, 
	[phone] varchar(30) NULL, 
	[city] varchar(100) NULL, 
	[province] varchar(50) NULL, 
	[postal_code] varchar(20) NULL, 
	[signup_date] date NULL, 
	[signup_year] int NULL, 
	[preferred_cuisine] varchar(100) NULL, 
	[loyalty_member] bit NULL, 
	[marketing_opt_in] bit NULL
);