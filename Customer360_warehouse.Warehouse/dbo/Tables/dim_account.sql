CREATE TABLE [dbo].[dim_account] (

	[account_key] bigint IDENTITY NOT NULL, 
	[account_id] varchar(50) NOT NULL, 
	[account_status] varchar(50) NULL, 
	[opened_date] date NULL, 
	[closed_date] date NULL, 
	[account_age_years] decimal(10,1) NULL, 
	[balance_band] varchar(50) NULL, 
	[is_active_account] bit NULL
);