/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
use dataWareHouse
go

IF OBJECT_ID('silver.crmCustInfo', 'U') IS NOT NULL
    DROP TABLE silver.crmCustInfo;
GO

CREATE TABLE silver.crmCustInfo (
    cstId              INT,
    cstKey             NVARCHAR(50),
    cstFirstname       NVARCHAR(50),
    cstLastname        NVARCHAR(50),
    cstMaritalStatus  NVARCHAR(50),
    cstGndr            NVARCHAR(50),
    cstCreateDate     DATE,
    dwhCreateDate    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crmPrdInfo', 'U') IS NOT NULL
    DROP TABLE silver.crmPrdInfo;
GO

CREATE TABLE silver.crmPrdInfo (
    prdId       INT,
    dwhCatId    NVARCHAR(50),
    prdKey      NVARCHAR(50),
    prdNm       NVARCHAR(50),
    prdCost     INT,
    prdLine     NVARCHAR(50),
    prdStartDt DATETIME,
    prdEndDt   DATETIME,
    dwhCreateDate DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crmSalesDetails', 'U') IS NOT NULL
    DROP TABLE silver.crmSalesDetails;
GO

CREATE TABLE silver.crmSalesDetails (
    slsOrdNum  NVARCHAR(50),
    slsPrdKey  NVARCHAR(50),
    slsCustId  INT,
    slsOrderDt DATE,
    slsShipDt  DATE,
    slsDueDt   DATE,
    slsSales    INT,
    slsQuantity INT,
    slsPrice    INT,
    dwhCreateDate DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erpLocA101', 'U') IS NOT NULL
    DROP TABLE silver.erpLocA101;
GO

CREATE TABLE silver.erpLocA101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    dwhCreateDate DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erpCustAz12', 'U') IS NOT NULL
    DROP TABLE silver.erpCustAz12;
GO

CREATE TABLE silver.erpCustAz12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwhCreateDate DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erpPxCatG1v2', 'U') IS NOT NULL
    DROP TABLE silver.erpPxCatG1v2;
GO

CREATE TABLE silver.erpPxCatG1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwhCreateDate DATETIME2 DEFAULT GETDATE()
);
GO

