-- About aggregation policy:
-- Snowflake offers a feature to share datasets while maintaining control over data usage through aggregation policies. 
-- These policies allow data owners to specify minimum group sizes for aggregation, enhancing data privacy and security.

-- Requirements:
-- Utilize SQL queries to aggregate data from the specified table or view.
-- Adhere to the allowed aggregation functions permitted by the aggregation policy.
-- Ensure that each group formed by the query contains at least the specified minimum number of records as per the 
-- aggregation policy.

-- Instructions:
-- Use the code below to create a sample table named Sales_Records
-- Define an aggregation policy with a minimum group size (X).
-- Create SQL queries that aggregate data from the chosen dataset, adhering to the aggregation policy requirements.
-- Test your queries to ensure compliance with the policy.

use database db_ff;
create or replace schema db_ff.week86;

-- Create the Sales_Records table
CREATE TABLE Sales_Records (
    Order_ID INT,
    Product_Name VARCHAR(50),
    Product_Category VARCHAR(50),
    Quantity INT,
    Unit_Price DECIMAL(10,2),
    Customer_ID INT
);

-- Insert sample data into the Sales_Records table
INSERT INTO Sales_Records (Order_ID, Product_Name, Product_Category, Quantity, Unit_Price, Customer_ID) VALUES
(1, 'Laptop', 'Electronics', 2, 1200.00, 101),
(2, 'Smartphone', 'Electronics', 1, 800.00, 102),
(3, 'Headphones', 'Electronics', 5, 50.00, 103),
(4, 'T-shirt', 'Apparel', 3, 20.00, 104),
(5, 'Jeans', 'Apparel', 2, 30.00, 105),
(6, 'Sneakers', 'Footwear', 1, 80.00, 106),
(7, 'Backpack', 'Accessories', 4, 40.00, 107),
(8, 'Sunglasses', 'Accessories', 2, 50.00, 108),
(9, 'Watch', 'Accessories', 1, 150.00, 109),
(10, 'Tablet', 'Electronics', 3, 500.00, 110),
(11, 'Jacket', 'Apparel', 2, 70.00, 111),
(12, 'Dress', 'Apparel', 1, 60.00, 112),
(13, 'Sandals', 'Footwear', 4, 25.00, 113),
(14, 'Belt', 'Accessories', 2, 30.00, 114),
(15, 'Speaker', 'Electronics', 1, 150.00, 115),
(16, 'Wallet', 'Accessories', 3, 20.00, 116),
(17, 'Hoodie', 'Apparel', 2, 40.00, 117),
(18, 'Running Shoes', 'Footwear', 1, 90.00, 118),
(19, 'Earrings', 'Accessories', 4, 15.00, 119),
(20, 'Ring', 'Accessories', 2, 50.00, 120);


-- create an aggregation policy
create or replace aggregation policy mypolicy
    as () returns aggregation_constraint -> aggregation_constraint(min_group_size => 3);

alter table Sales_Records set aggregation policy mypolicy;


-- verify the policy
-- allowed
select Product_Category, sum(Quantity) as Total_Quantity
from sales_records
group by Product_Category;

-- not allowed
select Product_Category, sum(Quantity*Unit_Price) as Total_Revenue
from sales_records
group by Product_Category;

-- allowed
select Product_Category, sum(Quantity)*avg(Unit_Price) as Approx_Total_Revenue 
from sales_records
group by Product_Category;

-- allowed (but only one row with null customer_id)
select Customer_ID, sum(Quantity) 
from sales_records
group by Customer_ID;
