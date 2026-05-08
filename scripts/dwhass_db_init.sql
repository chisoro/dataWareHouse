/*
=============================================================
Create Database and Schemas
=============================================================
Method:
	shrink and truncate. If the database exists, it is dropped and recreated. ( a new copy is always created)
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
     Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Create a temp table to store start time across batches
IF OBJECT_ID('tempdb..##tblStartTime') IS NOT NULL DROP TABLE ##tblStartTime;
CREATE TABLE ##tblStartTime (Val DATETIME);
INSERT INTO ##tblStartTime SELECT GETDATE();
GO

PRINT '====================================================================';
PRINT 'Creating Database DataWarehouse and Schemas: bronze, silver and gold';
PRINT '====================================================================';

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dataWareHouse')
BEGIN
    PRINT 'Dropping and recreating the DataWarehouse database';
    ALTER DATABASE dataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE dataWareHouse;
END;
GO

CREATE DATABASE dataWareHouse;
GO

USE dataWareHouse;
GO

PRINT 'Creating Schemas';
GO 

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

-- Calculate duration using the global temp table
DECLARE @startTime DATETIME = (SELECT Val FROM ##tblStartTime);
DECLARE @endTime DATETIME = GETDATE();

PRINT '>> Creation Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
PRINT '>> -------------------------------END';

-- Cleanup
DROP TABLE ##tblStartTime;
GO
