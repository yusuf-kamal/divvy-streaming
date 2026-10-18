-- Create the database if it does not exist
IF DB_ID('Bike_Streaming') IS NULL
    CREATE DATABASE Bike_Streaming;
GO

USE Bike_Streaming;
GO

-- Bronze layer: raw data as received, no business transformations
IF SCHEMA_ID('bronze') IS NULL
    EXEC('CREATE SCHEMA bronze');
GO

IF OBJECT_ID('bronze.station_status') IS NULL
CREATE TABLE bronze.station_status (
    id               BIGINT IDENTITY(1,1) PRIMARY KEY,
    network_id       NVARCHAR(50)  NOT NULL,
    station_id       NVARCHAR(64)  NOT NULL,
    name             NVARCHAR(200) NULL,
    latitude         FLOAT         NULL,
    longitude        FLOAT         NULL,
    free_bikes       INT           NULL,
    empty_slots      INT           NULL,
    source_timestamp DATETIME2(6)  NULL,     -- when the station updated at the source (UTC)
    ingested_at      DATETIME2(6)  NOT NULL, -- when the producer pulled it (UTC)
    loaded_at        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME() -- when it landed in SQL Server
);
GO

-- Typical query pattern: latest state per station / history over time
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_station_status_station_ingested'
               AND object_id = OBJECT_ID('bronze.station_status'))
CREATE INDEX IX_station_status_station_ingested
    ON bronze.station_status (station_id, ingested_at);
GO