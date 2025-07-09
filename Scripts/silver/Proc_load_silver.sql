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

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '===================================================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'LOADING CRM TABLE';
		PRINT '---------------------------------------------------';
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table1; silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,            
            cst_key,             
            cst_firstname,     
            cst_lastname,        
            cst_marital_status,  
            cst_gndr,            
            cst_create_date  
        )
        select cst_id,
               cst_key,
               TRIM(cst_firstname) AS cst_firstname, 
               TRIM(cst_lastname) AS cst_lastname,
               CASE 
                   WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                   WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                   ELSE 'n/a'
               END cst_marital_status,  -- Normalize marital status values to readable format
               CASE 
                   WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                   WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                   ELSE 'n/a'
               END cst_gndr,  -- Normalize gender valuer to readable format
               cst_create_date
        from (
             select *, 
             row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
             from bronze.crm_cust_info
             where cst_id is not null
        )t 
        where flag_last = 1; -- Select the most recent record of customer data
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table1; silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
         prd_id, 
         Replace(SUBSTRING(prd_key,1, 5), '-','_') AS cat_id, -- Extract category ID
         SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,      -- Extract product key
         prd_nm,
         iSNULL(prd_cost, 0) AS prd_cost,
         CASE UPPER(TRIM(prd_line))
              WHEN 'M' THEN 'MOUNTAIN'
              WHEN 'R' THEN 'ROAD'
              WHEN 'S' THEN 'Other Sales'
              WHEN 'T' THEN 'Touring'
              ELSE 'n/a'
         END AS prd_line,  -- Map product line codes to descriptive values
         cast(prd_start_dt as date) as prd_start_dt,
         cast(
              Lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 
              as date) 
              as prd_end_dt -- Calculate end date as one day before the next start date
         From bronze.crm_prd_info
         SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

        PRINT '>> Truncating Table1; silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data into: silver.crm_sales_details';
         SET @start_time = GETDATE();
         INSERT INTO silver.crm_sales_details(    
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price )
   
           SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) ! =8 Then NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) ! =8 Then NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) ! =8 Then NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt,
            case when sls_sales is NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
                 then sls_quantity * ABS(sls_price)
                 else sls_sales
             end as sls_sales,
             sls_quantity,
             case when sls_price is null or sls_price=0
                  then sls_price / NULLIF(sls_quantity, 0)
                  else sls_price
             end as sls_price
            from bronze.crm_sales_details
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

        PRINT '>> Truncating Table1; silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data into: silver.erp_cust_az12';
        SET @start_time = GETDATE();
        INSERT INTO silver.erp_cust_az12
        (cid,bdate,gen)
        select
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove 'NAS' prefix if present
                 ELSE cid
            END as cid,
            case when bdate > GETDATE() THEN NULL
                 else bdate
            end  as bdate, -- Set future birthdates to Null
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
                Else 'n/a'
            END AS gen --Normalize gender values and handle unknown cases
        from bronze.erp_cust_az12
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

        PRINT '>> Truncating Table1; silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data into: silver.erp_loc_a101';
        SET @start_time = GETDATE();
        INSERT INTO silver.erp_loc_a101
        (cid, cntry)
        select
        Replace(cid, '-', '') cid,
        CASE when Trim(cntry) = 'DE' THEN 'GERMANY'
             when Trim(cntry) IN ('US', 'USA') Then 'united states'
             when Trim(cntry) = '' OR cntry is null THEN 'n/a'
             else TRIM(cntry)
        END AS cntry
        from bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

        PRINT '>> Truncating Table1; silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
        SET @start_time = GETDATE();
        insert into silver.erp_px_cat_g1v2
        (id,cat,subcat,maintenance)
            select 
            id,
            cat,
            subcat,
            maintenance
        from bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'
		SET @batch_end_time = GETDATE();
        PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
    END TRY
    BEGIN CATCH
        PRINT '===================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';
    END CATCH
END



