/*
===============================================================================
Data Check
===============================================================================
Script Purpose:
    This script performs various checks for data consistency, accuracy, 
    and standardization across the 'bronze' layer to identify what needs to be cleaned. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

====
*/
use dataWareHouse
go

CREATE OR ALTER PROCEDURE bronze.dataCheckBronze AS
BEGIN
        PRINT '====================================================================';
        PRINT 'Checking bronze.crmCustInfo';
        PRINT '====================================================================';
        PRINT 'Check for NULLs or Duplicates in Primary Key';

        SELECT 
            cstID,
            COUNT(*) 
        FROM bronze.crmCustInfo
        GROUP BY cstID
        HAVING COUNT(*) > 1 OR cstID IS NULL;

        PRINT 'Check for Unwanted Spaces';
        SELECT 
            cstKey 
        FROM bronze.crmCustInfo
        WHERE cstKey != TRIM(cstKey);

        PRINT 'Data Standardization & Consistency';
        SELECT DISTINCT 
            cstMaritalStatus 
        FROM bronze.crmCustInfo;

        PRINT '====================================================================';
        PRINT 'Checking bronze.crmPrdInfo';
        PRINT '====================================================================';
        PRINT 'Check for NULLs or Duplicates in Primary Key';

        SELECT 
            prdId,
            COUNT(*) 
        FROM bronze.crmPrdInfo
        GROUP BY prdId
        HAVING COUNT(*) > 1 OR prdId IS NULL;

        PRINT 'Check for Unwanted Spaces';
        SELECT 
            prdNm 
        FROM bronze.crmPrdInfo
        WHERE prdNm != TRIM(prdNm);

        PRINT 'Check for NULLs or Negative Values in Cost';
        SELECT 
            prdCost 
        FROM bronze.crmPrdInfo
        WHERE prdCost < 0 OR prdCost IS NULL;

        PRINT 'Data Standardization & Consistency'
        SELECT DISTINCT 
            prdLine 
        FROM bronze.crmPrdInfo;

        PRINT 'Check for Invalid Date Orders (Start Date > End Date)';
        SELECT 
            * 
        FROM bronze.crmPrdInfo
        WHERE prdEndDt < prdStartDt;

        PRINT '====================================================================';
        PRINT 'Checking bronze.crmSalesDetails';
        PRINT '====================================================================';
        PRINT 'Check for Invalid Dates';

        SELECT 
            NULLIF(slsDueDt, 0) AS slsDueDt 
        FROM bronze.crmSalesDetails
        WHERE slsDueDt <= 0 
            OR LEN(slsDueDt) != 8 
            OR slsDueDt > 20500101 
            OR slsDueDt < 19000101;

        PRINT 'Check for Invalid Date Orders (Order Date > Shipping/Due Dates)';

        SELECT 
            * 
        FROM bronze.crmSalesDetails
        WHERE slsOrderDt > slsShipDt 
           OR slsOrderDt > slsDueDt;

        PRINT 'Check Data Consistency: Sales = Quantity * Price';

        SELECT DISTINCT 
            slsSales,
            slsQuantity,
            slsPrice 
        FROM bronze.crmSalesDetails
        WHERE slsSales != slsQuantity * slsPrice
           OR slsSales IS NULL 
           OR slsQuantity IS NULL 
           OR slsPrice IS NULL
           OR slsSales <= 0 
           OR slsQuantity <= 0 
           OR slsPrice <= 0
        ORDER BY slsSales, slsQuantity, slsPrice;

        PRINT '====================================================================';
        PRINT 'Checking bronze.erpCustAz12';
        PRINT '====================================================================';
        PRINT 'Identify Out-of-Range Dates';
        PRINT 'Expectation: Birthdates between 1924-01-01 and Today. We cannot expect to have a customer of over 103 years old';
        SELECT DISTINCT 
            bdate 
        FROM bronze.erpCustAz12
        WHERE bdate < '1924-01-01' 
           OR bdate > GETDATE();

        PRINT 'Data Standardization & Consistency';
        SELECT DISTINCT 
            gen 
        FROM bronze.erpCustAz12;

        PRINT '====================================================================';
        PRINT 'Checking bronze.erpLocA101';
        PRINT '====================================================================';
        PRINT 'Data Standardization & Consistency';
        SELECT DISTINCT 
            cntry 
        FROM bronze.erpLocA101
        ORDER BY cntry;

        PRINT '====================================================================';
        PRINT 'Checking bronze.erpPxCatG1v2';
        PRINT '====================================================================';
        PRINT 'Check for Unwanted Spaces';
        SELECT 
            * 
        FROM bronze.erpPxCatG1v2
        WHERE cat != TRIM(cat) 
           OR subcat != TRIM(subcat) 
           OR maintenance != TRIM(maintenance);

        PRINT 'Data Standardization & Consistency';
        SELECT DISTINCT 
            maintenance 
        FROM bronze.erpPxCatG1v2;
    end
    Go

    EXEC bronze.dataCheckBronze;

