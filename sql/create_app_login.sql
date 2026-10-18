USE master;
GO
IF SUSER_ID('divvy_app') IS NULL
    CREATE LOGIN divvy_app WITH PASSWORD = '$(APP_PASSWORD)', CHECK_POLICY = OFF;
GO
USE Bike_Streaming;
GO
IF USER_ID('divvy_app') IS NULL
    CREATE USER divvy_app FOR LOGIN divvy_app;
ALTER ROLE db_datareader ADD MEMBER divvy_app;
ALTER ROLE db_datawriter ADD MEMBER divvy_app;
GO