/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
    
===============================================================================
*/
use DataWarehouse
go

CREATE OR ALTER PROCEDURE bronze.loadBronze AS
BEGIN
	DECLARE @startTime DATETIME, @endTime DATETIME, @batchStartTime DATETIME, @batchEndTime DATETIME; 
	BEGIN TRY
		SET @batchStartTime = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.crmCustInfo';
		TRUNCATE TABLE bronze.crmCustInfo;
		PRINT '>> Inserting Data Into: bronze.crmCustInfo';
		BULK INSERT bronze.crmCustInfo
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.crmPrdInfo';
		TRUNCATE TABLE bronze.crmPrdInfo;

		PRINT '>> Inserting Data Into: bronze.crmPrdInfo';
		BULK INSERT bronze.crmPrdInfo
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.crmSalesDetails';
		TRUNCATE TABLE bronze.crmSalesDetails;
		PRINT '>> Inserting Data Into: bronze.crmSalesDetails';
		BULK INSERT bronze.crmSalesDetails
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.erpLocA101';
		TRUNCATE TABLE bronze.erpLocA101;
		PRINT '>> Inserting Data Into: bronze.erpLocA101';
		BULK INSERT bronze.erpLocA101
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.erpCustAz12';
		TRUNCATE TABLE bronze.erpCustAz12;
		PRINT '>> Inserting Data Into: bronze.erpCustAz12';
		BULK INSERT bronze.erpCustAz12
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: bronze.erpPxCatG1v2';
		TRUNCATE TABLE bronze.erpPxCatG1v2;
		PRINT '>> Inserting Data Into: bronze.erpPxCatG1v2';
		BULK INSERT bronze.erpPxCatG1v2
		FROM 'C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @startTime, @endTime) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batchEndTime = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batchStartTime, @batchEndTime) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

go

EXEC bronze.loadBronze;

