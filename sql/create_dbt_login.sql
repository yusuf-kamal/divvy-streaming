USE master;
GO
IF SUSER_ID('divvy_dbt') IS NULL
    CREATE LOGIN divvy_dbt WITH PASSWORD = '$(DBT_PASSWORD)', CHECK_POLICY = OFF;
GO
USE Bike_Streaming;
GO
IF USER_ID('divvy_dbt') IS NULL
    CREATE USER divvy_dbt FOR LOGIN divvy_dbt;
ALTER ROLE db_owner ADD MEMBER divvy_dbt;
GO