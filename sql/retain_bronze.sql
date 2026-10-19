USE Bike_Streaming;
GO

CREATE OR ALTER PROCEDURE bronze.retain_station_status( @retain_days INT = 3 )
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @cutoff     DATETIME2(6) = DATEADD(DAY, -@retain_days, SYSUTCDATETIME());
    DECLARE @cutoff_str VARCHAR(30)  = CONVERT(VARCHAR(30), @cutoff, 121);

    DELETE FROM bronze.station_status
    WHERE ingested_at < @cutoff;

    RAISERROR('Deleted rows ingested before %s (retained %d days)',
              0, 1,
              @cutoff_str, @retain_days) WITH NOWAIT;
END;
GO