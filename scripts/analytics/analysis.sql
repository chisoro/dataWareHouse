use dataWareHouse
go



       
        SELECT 
               p.category as Category_Name
              ,sum(s.salesAmount) as Total_Sales
        FROM [dataWareHouse].[gold].[factSales] s
        JOIN [dataWareHouse].[gold].[dimProducts] p  on s.productKey = p.productKey
        GROUP BY p.category
        ORDER BY Total_Sales DESC;


       
        SELECT 
            YEAR([orderDate]) AS Sales_Year, 
            MONTH([orderDate]) AS Sales_Month, 
            SUM([salesAmount]) AS Total_Revenue, 
            SUM([quantity]) AS Total_Quantity_Sold
        FROM [dataWareHouse].[gold].[factSales]
        WHERE [orderDate] IS NOT Null
        GROUP BY YEAR([orderDate]), MONTH([orderDate])
        ORDER BY Sales_Year, Sales_Month;


        
        SELECT TOP 10
            p.productName,
            p.productLine,
            SUM(s.salesAmount) AS Total_Revenue,
            SUM(s.quantity) AS Total_Quantity_Sold
        FROM [dataWareHouse].[gold].[factSales] s
        JOIN [dataWareHouse].[gold].[dimProducts] p ON s.productKey = p.productKey
        GROUP BY p.productname, p.productline
        ORDER BY Total_Revenue DESC;

        
        SELECT 
            c.country,
            COUNT(DISTINCT c.customerkey) AS Total_Customers,
            COUNT(DISTINCT f.orderNumber) AS Total_Orders,
            SUM(f.salesamount) AS Total_Revenue
        FROM [dataWareHouse].[gold].[dimCustomers] c
        JOIN [dataWareHouse].[gold].[factSales] f ON c.customerkey = f.customerkey
        GROUP BY c.country
        ORDER BY Total_Revenue DESC;

       
        SELECT 
            c.gender,
            AVG(f.salesamount) AS Avg_Order_Value
        FROM [dataWareHouse].[gold].[dimCustomers] c
        JOIN [dataWareHouse].[gold].[factSales] f ON c.customerkey = f.customerkey
        WHERE LOWER(c.gender) != 'n/a'
        GROUP BY c.gender;

        
        sELECT 
            p.subcategory AS Sub_Category,
            SUM(f.salesamount) AS Total_revenue,
            SUM(p.cost * f.quantity) AS Total_Cost,
            CAST(((100.0 * (SUM(f.salesamount) - SUM(p.cost * f.quantity))) / SUM(f.salesamount)) AS DECIMAL(10,2)) AS profit_margin_pct
        FROM [dataWareHouse].[gold].[dimProducts] p
        JOIN [dataWareHouse].[gold].[factSales] f ON p.productkey = f.productkey
        GROUP BY p.subcategory
        ORDER BY profit_margin_pct DESC;


        
        SELECT 
            p.category AS Category,
            AVG(DATEDIFF(day, f.orderdate, f.shippingdate)) AS Avg_Shipping_Delay_Days
        FROM [dataWareHouse].[gold].[dimProducts] p
        JOIN [dataWareHouse].[gold].[factSales] f ON p.productkey = f.productkey
        GROUP BY p.category
        ORDER BY avg_shipping_delay_days DESC;



