USE master;
GO
IF SUSER_ID('divvy_bi') IS NULL
    CREATE LOGIN divvy_bi WITH PASSWORD = '$(BI_PASSWORD)', CHECK_POLICY = OFF;
GO
USE Bike_Streaming;
GO
IF USER_ID('divvy_bi') IS NULL
    CREATE USER divvy_bi FOR LOGIN divvy_bi;
GO
-- The gold schema is normally created by dbt; make sure it exists on a fresh volume.
IF SCHEMA_ID('gold') IS NULL EXEC('CREATE SCHEMA gold');
GO
-- Schema-level grant survives dbt dropping and recreating the tables.
GRANT SELECT ON SCHEMA::gold TO divvy_bi;
GO