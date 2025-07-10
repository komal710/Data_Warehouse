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
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
Select
    ROW_NUMBER() over(order by cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is themaster for gender Info
          ELSE COALESCE(ca.gen, 'n/a')              -- Fallback to ERP data
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
     ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
     ON ci.cst_key = la.cid
GO



-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
select
    ROW_NUMBER() over(order by pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id As product_id,
	pn.prd_key As product_number,
	pn.prd_nm As product_name,
	pn.cat_id As category_id,
	pc.cat As category,
	pc.subcat As subcategory,
	pc.maintenance ,
	pn.prd_cost As product_cost,
	pn.prd_line As product_line,
	pn.prd_start_dt As product_start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
     on pn.cat_id = pc.id
where prd_end_dt is NULL --filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view gold.fact_sales as
select 
    sd.sls_ord_num as order_number,
    pr.product_key as product_key,
    cu.customer_key as customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
    on sd.sls_cust_id = cu.customer_id
GO