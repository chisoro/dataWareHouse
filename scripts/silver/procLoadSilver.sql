/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.


    EXEC silver.loadSilver;
===============================================================================
*/
use dataWareHouse
go
CREATE OR ALTER PROCEDURE silver.loadSilver AS
BEGIN
    DECLARE @startTime DATETIME, @endTime DATETIME, @batchStartTime DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batchStartTime = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crmCustInfo
        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.crmCustInfo';
		TRUNCATE TABLE silver.crmCustInfo;
		PRINT '>> Inserting Data Into: silver.crmCustInfo';
		INSERT INTO silver.crmCustInfo (
			cstId, 
			cstKey, 
			cstFirstname, 
			cstLastname, 
			cstMaritalStatus, 
			cstGndr,
			cstCreateDate
		)
		SELECT
			cstId,
			cstKey,
			TRIM(cstFirstname) AS cstFirstname,
			TRIM(cstLastname) AS cstLastname,
			CASE 
				WHEN UPPER(TRIM(cstMaritalStatus)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cstMaritalStatus)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cstMaritalStatus, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cstGndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cstGndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cstGndr, -- Normalize gender values to readable format
			cstCreateDate
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cstId ORDER BY cstCreateDate DESC) AS flatLast
			FROM bronze.crmCustInfo
			WHERE cstId IS NOT NULL
		) t
		WHERE flatLast = 1; -- Select the most recent record per customer
		SET @endTime = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crmPrdInfo
		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.crmPrdInfo';
		TRUNCATE TABLE silver.crmPrdInfo;
		PRINT '>> Inserting Data Into: silver.crmPrdInfo';
		INSERT INTO silver.crmPrdInfo (
			prdId, 
			dwhCatId, 
			prdKey, 
			prdNm, 
			prdCost, 
			prdLine, 
			prdStartDt, 
			prdEndDt
		)
		SELECT
			prdId,
			REPLACE(SUBSTRING(prdKey, 1, 5), '-', '_') AS dwhCatId,
			SUBSTRING(prdKey, 7, LEN(prdKey)) AS prdKey,
			prdNm,
			ISNULL(prdCost, 0) AS prdCost,
			CASE
				WHEN UPPER(TRIM(prdLine)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prdLine)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prdLine)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prdLine)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prdLine,
			CAST(CAST(prdStartDt AS VARCHAR) AS DATE) AS prdStartDt,
			CAST(DATEADD(DAY, -1, LEAD(CAST(CAST(prdStartDt AS VARCHAR) AS DATE)) OVER (PARTITION BY prdKey ORDER BY prdStartDt)) AS DATE) AS prdEndDt
		FROM bronze.crmPrdInfo;

        SET @endTime = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crmSalesDetails
        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.crmSalesDetails';
		TRUNCATE TABLE silver.crmSalesDetails;
		PRINT '>> Inserting Data Into: silver.crmSalesDetails';
		INSERT INTO silver.crmSalesDetails (
			slsOrdNum,
			slsPrdKey,
			slsCustId,
			slsOrderDt,
			slsShipDt,
			slsDueDt,
			slsSales,
			slsQuantity,
			slsPrice
		)
		SELECT 
			slsOrdNum,
			slsPrdKey,
			slsCustId,
			CASE 
			WHEN slsOrderDt = 0 OR LEN(CAST(slsOrderDt AS VARCHAR)) != 8 THEN NULL
				ELSE CONVERT(DATE, CAST(slsOrderDt AS VARCHAR), 112)
			END AS slsOrderDt,
			CASE 
				WHEN slsShipDt = 0 OR LEN(CAST(slsShipDt AS VARCHAR)) != 8 THEN NULL
				ELSE CONVERT(DATE, CAST(slsShipDt AS VARCHAR), 112)
			END AS slsShipDt,
			CASE 
				WHEN slsDueDt = 0 OR LEN(CAST(slsDueDt AS VARCHAR)) != 8 THEN NULL
				ELSE CONVERT(DATE, CAST(slsDueDt AS VARCHAR), 112)
			END AS slsDueDt,
			CASE 
				WHEN slsSales IS NULL OR slsSales <= 0 OR slsSales != slsQuantity * ABS(slsPrice) 
					THEN slsQuantity * ABS(slsPrice)
				ELSE slsSales
			END AS slsSales, -- Recalculate sales if original value is missing or incorrect
			slsQuantity,
			CASE 
				WHEN slsPrice IS NULL OR slsPrice <= 0 
					THEN slsSales / NULLIF(slsQuantity, 0)
				ELSE slsPrice  -- Derive price if original value is invalid
			END AS slsPrice
		FROM bronze.crmSalesDetails;
        SET @endTime = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erpCustAz12
        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.erpCustAz12';
		TRUNCATE TABLE silver.erpCustAz12;
		PRINT '>> Inserting Data Into: silver.erpCustAz12';
		INSERT INTO silver.erpCustAz12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				when bdate < '1900-01-01' THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erpCustAz12;
	    SET @endTime = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erpLocA101
        SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.erpLocA101';
		TRUNCATE TABLE silver.erpLocA101;
		PRINT '>> Inserting Data Into: silver.erpLocA101';
		INSERT INTO silver.erpLocA101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erpLocA101;
	    SET @endTime = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erpPxCatG1v2
		SET @startTime = GETDATE();
		PRINT '>> Truncating Table: silver.erpPxCatG1v2';
		TRUNCATE TABLE silver.erpPxCatG1v2;
		PRINT '>> Inserting Data Into: silver.erpPxCatG1v2';
		INSERT INTO silver.erpPxCatG1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erpPxCatG1v2;
		SET @endTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @startTime, @endTime) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batchStartTime, @batch_end_time) AS NVARCHAR) + ' seconds';
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

EXEC silver.loadSilver;