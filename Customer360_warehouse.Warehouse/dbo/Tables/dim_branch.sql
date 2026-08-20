CREATE TABLE [dbo].[dim_branch] (

	[branch_key] bigint IDENTITY NOT NULL, 
	[branch_code] varchar(50) NOT NULL, 
	[branch_name] varchar(150) NOT NULL, 
	[city] varchar(100) NULL, 
	[state] varchar(100) NULL, 
	[country] varchar(100) NULL, 
	[region] varchar(100) NULL, 
	[branch_type] varchar(50) NULL, 
	[is_active_branch] bit NULL
);