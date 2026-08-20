CREATE TABLE [dbo].[fact_support_ticket] (

	[support_ticket_key] bigint IDENTITY NOT NULL, 
	[ticket_id] varchar(50) NOT NULL, 
	[customer_key] bigint NOT NULL, 
	[support_category_key] bigint NOT NULL, 
	[ticket_date_key] int NULL, 
	[resolution_date_key] int NULL, 
	[resolution_days] int NULL, 
	[ticket_age_days] int NULL, 
	[sla_breached_flag] bit NULL, 
	[is_open_ticket] bit NULL
);