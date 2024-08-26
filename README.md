Create database ECommerce;
use ecommerce;

CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    CompanyName VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(30),
    PostalCode VARCHAR(60),
    Country VARCHAR(50),
    Phone VARCHAR(15),
    Email VARCHAR(25)
);

CREATE TABLE Payments (
    PaymentID INT PRIMARY KEY,
    PaymentType VARCHAR(50),
    Allowed varchar(3)
);

CREATE TABLE Shippers (
    ShipperID INT PRIMARY KEY,
    CompanyName VARCHAR(50),
    Phone VARCHAR(25)
);

CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(50),
    Active VARCHAR(3)
);

CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(30),
    Country VARCHAR(60),
    PostalCode VARCHAR(50),
    Phone VARCHAR(15),
    Email VARCHAR(50),
    DateEntered VARCHAR(15)
);

CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    Product VARCHAR(100),
    CategoryID VARCHAR(50),
    Sub_Category VARCHAR(50),
    Brand VARCHAR(50),
    Sale_Price FLOAT,
    Market_Price FLOAT,
    Type VARCHAR(100)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    PaymentID INT,
    OrderDate date,
    ShipperID INT,
    ShipDate DATE,
    DeliveryDate DATE,
    Total_order_amount DECIMAL(10, 2)
);

CREATE TABLE OrderDetails (
    OrderDetailID INT AUTO_INCREMENT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    SupplierID INT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
    INDEX idx_order_id (OrderID),
    INDEX idx_product_id (ProductID),
    INDEX idx_supplier_id (SupplierID)
);

select * from categories;
select * from customers;
select * from orderdetails;
select * from orders;
select * from payments;
select * from products;
select * from shippers;
select * from suppliers;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ecommerce sales analysis/orderdetails.csv'
INTO TABLE orderdetails
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

INSERT INTO suppliers (SupplierID, CompanyName, City, State, PostalCode, Country, Phone, Email)
VALUES
(1, 'Sunshine Electronics Pvt.Ltd.', 'Fremont', 'California', '841726', 'USA','8796300412', 'contactus@sunelec.com'),
(2, 'Tasha Apparel', 'Abu Dhabhi', 'Abu Dhabhi', '683008', 'UAE', '8773744676', 'customercare@tashaapparel.com'),
(3, 'Alpha imports', 'Austin', 'Texas', '889037', 'USA', '9872259654', 'customers@alphaimports.com'),
(4, 'GreenDroShip', 'Bangalore', 'Karnataka', '416556', 'India', '8245273210', 'gogreen@greendrop.co.in'),
(5, 'Megagoods', 'Kolkata', 'West Bengal', '119658', 'India', '8568274416', 'gobig@megagoods.co.in'),
(6, 'ASI Partners', 'New York City', 'New York', '865066', 'USA', '9363378404', 'contactus@asi.com');

-- 1. Create a YOY analysis for the count of customers enrolled with the company each month. 
SELECT 
    extract( Month from DateEntered) AS Month,
    Count(CASE WHEN extract( Year from DateEntered) = 2020 THEN customerId end ) AS Year_2020,
    Count(CASE WHEN extract( Year from DateEntered) = 2021 THEN customerId end) AS Year_2021
FROM Customers
GROUP BY Month;


-- 2. Find out the top 4 best-selling products in each of the categories (in the categories that are currently active on the Website).
WITH CTE AS (
    SELECT 
        p.Product AS ProductName, c.CategoryName,
        SUM(od.Quantity) AS TotalQuantitySold, 
        SUM(od.Quantity * p.Sale_Price) AS TotalRevenue
    FROM Products p
    INNER JOIN OrderDetails od ON p.ProductID = od.ProductID
    INNER JOIN Categories c ON c.CategoryID = p.CategoryID
    WHERE c.Active = 'Yes'
    GROUP BY p.Product, c.CategoryName
),
CTE2 AS (
    SELECT ProductName, CategoryName, TotalQuantitySold, TotalRevenue,
        DENSE_RANK() OVER (PARTITION BY CategoryName ORDER BY TotalRevenue DESC) AS rnk
    FROM 
        CTE)
SELECT 
    CategoryName,ProductName,TotalQuantitySold,TotalRevenue
FROM CTE2
WHERE rnk <= 5
ORDER BY CategoryName, rnk;


-- 3. Find out the least selling products in each of the categories (in the Categories that are currently active on the website).
WITH CTE AS (
    SELECT 
        p.Product AS ProductName, c.CategoryName,
        SUM(od.Quantity) AS TotalQuantitySold, 
        SUM(od.Quantity * p.Sale_Price) AS TotalRevenue
    FROM Products p
    INNER JOIN OrderDetails od ON p.ProductID = od.ProductID
    INNER JOIN Categories c ON c.CategoryID = p.CategoryID
    WHERE c.Active = 'Yes'
    GROUP BY p.Product, c.CategoryName
),
CTE2 AS (
    SELECT ProductName, CategoryName, TotalQuantitySold, TotalRevenue,
        DENSE_RANK() OVER (PARTITION BY CategoryName ORDER BY TotalRevenue asc) AS rnk
    FROM 
        CTE)
SELECT 
    CategoryName,ProductName,TotalQuantitySold,TotalRevenue
FROM CTE2
WHERE rnk = 1
ORDER BY CategoryName, rnk;


-- 4. Find the cumulative sum of total orders placed for the year 2021 
	-- (using window function)
SELECT 
    OrderDate, SUM(Total_order_amount) AS DailyTotal,
    SUM(SUM(Total_order_amount)) OVER (ORDER BY OrderDate) AS CumulativeTotal
FROM  Orders
WHERE YEAR(OrderDate) = 2021
GROUP BY OrderDate
ORDER BY OrderDate;

#--	using self join  # in SQL for cumulative we can use self join and give condition as table 2 <= table 1 to join
 SELECT o1.OrderDate,
    SUM(o1.Total_order_amount) AS DailyTotal,
    SUM(o2.Total_order_amount) AS CumulativeTotal
FROM Orders o1
INNER JOIN Orders o2 
	ON o2.OrderDate <= o1.OrderDate
WHERE YEAR(o1.OrderDate) = 2021 AND YEAR(o2.OrderDate) = 2021
GROUP BY o1.OrderDate
ORDER BY o1.OrderDate;

-- 5.find out the impact of running a campaign during July’21-Oct’21 what was the total sales generated for the categories“Foodgrains, Oil & Masala” and “Fruits & Vegetables” by entire customer base
 SELECT 
    c.CategoryName,
    SUM(od.Quantity * p.Sale_Price) AS TotalSales
FROM Orders o
INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
INNER JOIN products p ON od.ProductID = p.ProductID
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
WHERE 
    c.CategoryName IN ('Foodgrains, Oil & Masala', 'Fruits & Vegetables')
    AND o.OrderDate BETWEEN '2021-07-01' AND '2021-10-31'
GROUP BY c.CategoryName;

 
 -- 6. 	Create a Quarter-wise ranking in terms of revenue generated in each category in Year 2021
WITH RevenueByQuarter AS (
    SELECT c.CategoryName, QUARTER(o.OrderDate) AS Quarter,
	SUM(od.Quantity * p.Sale_Price) AS TotalRevenue
    FROM Orders o
    INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
    INNER JOIN Products p ON od.ProductID = p.ProductID
    INNER JOIN Categories c ON p.CategoryID = c.CategoryID
    WHERE YEAR(o.OrderDate) = 2021
    GROUP BY c.CategoryName, QUARTER(o.OrderDate)
)
SELECT CategoryName, Quarter, TotalRevenue,
    RANK() OVER (PARTITION BY Quarter ORDER BY TotalRevenue DESC) AS RevenueRank
FROM RevenueByQuarter
ORDER BY Quarter, RevenueRank;

-- 7. Find the top 3 Shipper companies in terms of : Average delivery time for each category for the latest year   
WITH AvgDeliveryTime AS (
    SELECT 
        s.CompanyName AS Shipper,
        c.CategoryName,
        EXTRACT(YEAR FROM o.DeliveryDate) AS Year,
        AVG(DATEDIFF(o.DeliveryDate, o.ShipDate)) AS AvgDeliveryTime,
        RANK() OVER (PARTITION BY c.CategoryName ORDER BY AVG(DATEDIFF(o.DeliveryDate, o.ShipDate)) ASC) AS DeliveryRank
    FROM Orders o
    INNER JOIN Shippers s ON o.ShipperID = s.ShipperID
    INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
    INNER JOIN Products p ON od.ProductID = p.ProductID
    INNER JOIN Categories c ON p.CategoryID = c.CategoryID
    WHERE EXTRACT(YEAR FROM o.DeliveryDate) = (SELECT MAX(EXTRACT(YEAR FROM DeliveryDate)) FROM Orders)
    GROUP BY 
        s.CompanyName, c.CategoryName, EXTRACT(YEAR FROM o.DeliveryDate)
)
SELECT Shipper, CategoryName, Year, AvgDeliveryTime,DeliveryRank
FROM AvgDeliveryTime
WHERE DeliveryRank <= 3
ORDER BY CategoryName, DeliveryRank;


-- 8. Find the top 25 customers in terms of: Total no. of orders placed for Year 2020
SELECT c.CustomerID, c.FirstName, c.LastName,
    COUNT(o.OrderID) AS TotalOrders
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE YEAR(o.OrderDate) = 2020
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalOrders DESC
LIMIT 25;

-- 9. How many new customers were acquired each month in the past year?
SELECT 
    EXTRACT(YEAR FROM DateEntered) AS Year,
    EXTRACT(MONTH FROM DateEntered) AS Month,
    COUNT(CustomerID) AS NewCustomers
FROM Customers
WHERE YEAR(CURRENT_DATE)-1
GROUP BY EXTRACT(YEAR FROM DateEntered), EXTRACT(MONTH FROM DateEntered)
ORDER BY Year, Month;

-- 10. Find the 3-day rolling average for the total purchase amount by each customer.
WITH DailyTotal AS (
    SELECT o.CustomerID, o.OrderDate,
        SUM(o.Total_order_amount) AS DailyTotalAmount
    FROM Orders o
    GROUP BY o.CustomerID, o.OrderDate
),
RollingAverage AS (
    SELECT CustomerID, OrderDate, DailyTotalAmount,
        AVG(DailyTotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS Rolling3DayAverage
    FROM DailyTotal
)
SELECT 
    CustomerID, OrderDate, DailyTotalAmount, Rolling3DayAverage
FROM RollingAverage
ORDER BY CustomerID, OrderDate;


-- 11. Create a Procedure to filter the orders from Orders table where total purchase amount in each order id < @x and purchase of year is @Y where @x and @y are the inputs provided by the user.
DELIMITER //
CREATE PROCEDURE FilterOrdersByAmountAndYear(
    IN x DECIMAL(10, 2), 
    IN y INT
)
BEGIN
    SELECT 
        OrderID, CustomerID,PaymentID,OrderDate,ShipperID,ShipDate,DeliveryDate,Total_order_amount
    FROM Orders
    WHERE Total_order_amount < x AND YEAR(OrderDate) = y;
END //
DELIMITER ;

CALL FilterOrdersByAmountAndYear(1000.00, 2020);

-- 12. Create a Trigger which creates a log when a new record is inserted into the Shippers table
CREATE TABLE ShippersLog (
    LogID INT AUTO_INCREMENT PRIMARY KEY,
    ShipperID INT,
    CompanyName VARCHAR(50),
    Phone VARCHAR(25),
    InsertedAt DATETIME);
DELIMITER //
CREATE TRIGGER AfterShipperInsert
AFTER INSERT ON Shippers
FOR EACH ROW
BEGIN
    INSERT INTO ShippersLog (ShipperID, CompanyName, Phone, InsertedAt)
    VALUES (NEW.ShipperID, NEW.CompanyName, NEW.Phone, NOW());
END //
DELIMITER ;
Insert into Shippers values (11, 'Asawari co.ltd',8484044787);
Insert into Shippers values (12, 'Alphonso company',7978372739);
Insert into Shippers values (13, 'Cheesecake factory',8484044787);
select * from ShippersLog;








