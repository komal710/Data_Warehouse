EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time = GETDATE();
		PRINT '===================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '===================================================';

		PRINT '---------------------------------------------------';
		PRINT 'LOADING CRM TABLE';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

		PRINT '---------------------------------------------------';
		PRINT 'LOADING ERP TABLE';
		PRINT '---------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE();
		PRINT '>> Inserting Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + Cast(datediff(second, @start_time,@end_time) as nvarchar) + ' seconds';
		PRINT '---------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\SQL-data-warehouse\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE()
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