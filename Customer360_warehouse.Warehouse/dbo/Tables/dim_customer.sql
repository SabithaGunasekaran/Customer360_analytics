CREATE TABLE [dbo].[dim_customer] (

	[customer_key] bigint IDENTITY NOT NULL, 
	[customer_id] varchar(50) NOT NULL, 
	[first_name] varchar(100) NULL, 
	[last_name] varchar(100) NULL, 
	[full_name] varchar(250) NULL, 
	[email] varchar(255) NULL, 
	[phone] varchar(50) NULL, 
	[gender] varchar(20) NULL, 
	[date_of_birth] date NULL, 
	[age] int NULL, 
	[age_group] varchar(20) NULL, 
	[city] varchar(100) NULL, 
	[state] varchar(100) NULL, 
	[country] varchar(100) NULL, 
	[customer_status] varchar(50) NULL, 
	[is_active_customer] bit NULL, 
	[customer_tenure_years] decimal(10,1) NULL, 
	[effective_from] datetime2(6) NOT NULL, 
	[effective_to] datetime2(6) NULL, 
	[is_current] bit NOT NULL
);