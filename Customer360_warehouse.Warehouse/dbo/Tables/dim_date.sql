CREATE TABLE [dbo].[dim_date] (

	[date_key] int NOT NULL, 
	[date_value] date NOT NULL, 
	[year] int NULL, 
	[quarter_number] int NULL, 
	[month_number] int NULL, 
	[month_name] varchar(20) NULL, 
	[day_of_month] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(20) NULL, 
	[week_number] int NULL, 
	[is_weekend] bit NULL
);