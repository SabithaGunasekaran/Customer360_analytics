CREATE TABLE [dbo].[fact_marketing_interaction] (

	[marketing_interaction_key] bigint IDENTITY NOT NULL, 
	[interaction_id] varchar(50) NOT NULL, 
	[customer_key] bigint NOT NULL, 
	[campaign_key] bigint NOT NULL, 
	[interaction_date_key] int NULL, 
	[responded_flag] bit NULL, 
	[days_since_interaction] int NULL
);