/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dimCustomers
-- =============================================================================
IF OBJECT_ID('gold.dimCustomers', 'V') IS NOT NULL
    DROP VIEW gold.dimCustomers;
GO

CREATE VIEW gold.dimCustomers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cstId) AS customerKey, -- Surrogate key
    ci.cstId                          AS customerId,
    ci.cstKey                         AS customerNumber,
    ci.cstFirstname                   AS firstName,
    ci.cstLastname                    AS lastName,
    la.cntry                           AS country,
    ci.cstMaritalStatus              AS maritalStatus,
    CASE 
        WHEN ci.cstGndr != 'n/a' THEN ci.cstGndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cstCreateDate                 AS createDate
FROM silver.crmCustInfo ci
LEFT JOIN silver.erpCustAz12 ca
    ON ci.cstKey = ca.cid
LEFT JOIN silver.erpLocA101 la
    ON ci.cstKey = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dimProducts
-- =============================================================================
IF OBJECT_ID('gold.dimProducts', 'V') IS NOT NULL
    DROP VIEW gold.dimProducts;
GO

CREATE VIEW gold.dimProducts AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prdStartDt, pn.prdKey) AS productKey, -- Surrogate key
    pn.prdId       AS productId,
    pn.prdKey      AS productNumber,
    pn.prdNm       AS productName,
    pn.dwhCatId       AS categoryId,
    pc.cat          AS category,
    pc.subcat       AS subCategory,
    pc.maintenance  AS maintenance,
    pn.prdCost     AS cost,
    pn.prdLine     AS productLine,
    pn.prdStartDt AS startDate
FROM silver.crmPrdInfo pn
LEFT JOIN silver.erpPxCatG1v2 pc
    ON pn.dwhCatId = pc.id
WHERE pn.prdEndDt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.factSales
-- =============================================================================
IF OBJECT_ID('gold.factSales', 'V') IS NOT NULL
    DROP VIEW gold.factSales;
GO

CREATE VIEW gold.factSales AS
SELECT
    sd.slsOrdNum  AS orderNumber,
    pr.productKey  AS productKey,
    cu.customerKey AS customerKey,
    sd.slsOrderDt AS orderDate,
    sd.slsShipDt  AS shippingDate,
    sd.slsDueDt   AS dueDate,
    sd.slsSales    AS salesAmount,
    sd.slsQuantity AS quantity,
    sd.slsPrice    AS price
FROM silver.crmSalesDetails sd
LEFT JOIN gold.dimProducts pr
    ON sd.slsPrdKey = pr.productNumber
LEFT JOIN gold.dimCustomers cu
    ON sd.slsCustId = cu.customerId;
GO
