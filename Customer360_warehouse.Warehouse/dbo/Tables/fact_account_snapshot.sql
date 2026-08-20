CREATE TABLE [dbo].[fact_account_snapshot] (

	[account_snapshot_key] bigint IDENTITY NOT NULL, 
	[customer_key] bigint NOT NULL, 
	[account_key] bigint NOT NULL, 
	[snapshot_date_key] int NOT NULL, 
	[branch_key] bigint NOT NULL, 
	[account_type_key] bigint NOT NULL, 
	[balance] decimal(18,2) NULL, 
	[is_active_account] bit NULL
);