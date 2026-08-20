CREATE TABLE [dbo].[dim_support_category] (

	[support_category_key] bigint IDENTITY NOT NULL, 
	[category] varchar(100) NULL, 
	[priority] varchar(50) NULL, 
	[sla_target_days] int NULL
);