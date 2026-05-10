use dataWareHouse
go
-- 1. Enable advanced options to be changed
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

-- 2. Enable Ad Hoc Distributed Queries
EXEC sp_configure 'Ad Hoc Distributed Queries', 1;
RECONFIGURE;
go

CREATE OR ALTER PROCEDURE report AS
BEGIN
        PRINT 'Sales grouped by Category ordered in descending order of revenue';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Category$]')
        SELECT 
               p.category as Category_Name
              ,sum(s.salesAmount) as Total_Sales
        FROM [dataWareHouse].[gold].[factSales] s
        JOIN [dataWareHouse].[gold].[dimProducts] p  on s.productKey = p.productKey
        GROUP BY p.category
        ORDER BY Total_Sales DESC;


        PRINT 'sales by year,month';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Monthly$]')
        SELECT 
            YEAR([orderDate]) AS Sales_Year, 
            MONTH([orderDate]) AS Sales_Month, 
            SUM([salesAmount]) AS Total_Revenue, 
            SUM([quantity]) AS Total_Quantity_Sold
        FROM [dataWareHouse].[gold].[factSales]
        WHERE [orderDate] != NULL
        GROUP BY YEAR([orderDate]), MONTH([orderDate])
        ORDER BY Sales_Year, Sales_Month;


        PRINT 'Top 10 product Sales';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Topproducts$]')
        SELECT TOP 10
            p.productName,
            p.productLine,
            SUM(s.salesAmount) AS Total_Revenue,
            SUM(s.quantity) AS Total_Quantity_Sold
        FROM [dataWareHouse].[gold].[factSales] s
        JOIN [dataWareHouse].[gold].[dimProducts] p ON s.productKey = p.productKey
        GROUP BY p.productname, p.productline
        ORDER BY Total_Revenue DESC;

        PRINT 'Customer segemenation by country';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Segmentation$]')
        SELECT 
            c.country,
            COUNT(DISTINCT c.customerkey) AS Total_Customers,
            COUNT(f.saleskey) AS Total_Orders,
            SUM(f.salesamount) AS Total_Revenue
        FROM [dataWareHouse].[gold].[dimCustomers] c
        JOIN [dataWareHouse].[gold].[factSales] f ON c.customerkey = f.customerkey
        GROUP BY c.country
        ORDER BY Total_Revenue DESC;

        PRINT 'Average order value by gender';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Segmentation$]')
        SELECT 
            c.gender,
            AVG(f.salesamount) AS Avg_Order_Value
        FROM [dataWareHouse].[gold].[dimCustomers] c
        JOIN [dataWareHouse].[gold].[factSales] f ON c.customerkey = f.customerkey
        WHERE LOWER(c.gender) != 'n/a'
        GROUP BY c.gender;

        PRINT 'PRODUCT SUBCATEGORY PROFITABILITY ANALYSIS';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Subcatory$]')
        SELECT 
            p.subcategory AS Sub_Category,
            SUM(f.salesamount) AS Total_revenue,
            SUM(p.cost * f.quantity) AS Total_Cost,
            ((SUM(f.salesamount) - SUM(p.cost * f.quantity)) / SUM(f.salesamount)) * 100 AS Profit_Margin_Pct
        FROM [dataWareHouse].[gold].[dimProducts] p
        JOIN [dataWareHouse].[gold].[factSales] f ON p.productkey = f.productkey
        GROUP BY p.subcategory
        ORDER BY profit_margin_pct DESC;


        PRINT 'Shipping delay analysis';
        INSERT INTO OPENROWSET('Microsoft.ACE.OLEDB.12.0', 
        'Excel 12.0;Database=C:\Users\ADMIN\OneDrive\Documents\msu\DS1.2\DSI143-Data Wahrehouse\DHWASSI\results\Analysis.xlsx;', 
        'SELECT * FROM [Shipping$]')
        SELECT 
            p.category AS Category,
            AVG(DATEDIFF(day, f.orderdate, f.shippingdate)) AS Avg_Shipping_Delay_Days
        FROM [dataWareHouse].[gold].[dimProducts] p
        JOIN [dataWareHouse].[gold].[factSales] f ON p.productkey = f.productkey
        GROUP BY p.category
        ORDER BY avg_shipping_delay_days DESC;
END

EXEC report;
