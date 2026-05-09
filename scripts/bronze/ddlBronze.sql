/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
USE dataWareHouse;
GO


IF OBJECT_ID('bronze.crmCustInfo', 'U') IS NOT NULL
    DROP TABLE bronze.crmCustInfo;
GO

CREATE TABLE bronze.crmCustInfo (
    cstId              INT,
    cstKey             NVARCHAR(50),
    cstFirstname       NVARCHAR(50),
    cstLastname        NVARCHAR(50),
    cstMaritalStatus  NVARCHAR(50),
    cstGndr            NVARCHAR(50),
    cstCreateDate     DATE
);
GO

IF OBJECT_ID('bronze.crmPrdInfo', 'U') IS NOT NULL
    DROP TABLE bronze.crmPrdInfo;
GO

CREATE TABLE bronze.crmPrdInfo (
    prdId       INT,
    prdKey      NVARCHAR(50),
    prdNm       NVARCHAR(50),
    prdCost     INT,
    prdLine     NVARCHAR(50),
    prdStartDt DATETIME,
    prdEndDt   DATETIME
);
GO

IF OBJECT_ID('bronze.crmSalesDetails', 'U') IS NOT NULL
    DROP TABLE bronze.crmSalesDetails;
GO

CREATE TABLE bronze.crmSalesDetails (
    slsOrdNum  NVARCHAR(50),
    slsPrdKey  NVARCHAR(50),
    slsCustId  INT,
    slsOrderDt INT,
    slsShipDt  INT,
    slsDueDt   INT,
    slsSales    INT,
    slsQuantity INT,
    slsPrice    INT
);
GO

IF OBJECT_ID('bronze.erpLocA101', 'U') IS NOT NULL
    DROP TABLE bronze.erpLocA101;
GO

CREATE TABLE bronze.erpLocA101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erpCustAz12', 'U') IS NOT NULL
    DROP TABLE bronze.erpCustAz12;
GO

CREATE TABLE bronze.erpCustAz12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erpPxCatG1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erpPxCatG1v2;
GO

CREATE TABLE bronze.erpPxCatG1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO
