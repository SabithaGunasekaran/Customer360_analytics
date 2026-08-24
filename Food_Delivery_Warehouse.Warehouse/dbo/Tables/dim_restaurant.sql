CREATE TABLE [dbo].[dim_restaurant] (

	[restaurant_key] bigint IDENTITY NOT NULL, 
	[restaurant_id] varchar(50) NOT NULL, 
	[restaurant_name] varchar(200) NULL, 
	[cuisine_type] varchar(100) NULL, 
	[city] varchar(100) NULL, 
	[restaurant_rating] decimal(3,1) NULL
);