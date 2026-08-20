CREATE TABLE [dbo].[dim_campaign] (

	[campaign_key] bigint IDENTITY NOT NULL, 
	[campaign] varchar(200) NULL, 
	[channel] varchar(50) NULL, 
	[interaction_type] varchar(100) NULL, 
	[digital_channel_flag] bit NULL
);