/* ============================================================
   Procedure: bronze.load_bronze
   Layer: Bronze (Raw Ingestion)

   Description:
   Loads raw data from CRM and ERP source files into Bronze
   layer tables using BULK INSERT. The procedure truncates
   target tables before loading to ensure a clean, repeatable
   batch ingestion.

   Source Systems:
   - CRM: Customer, Product, Sales data
   - ERP: Customer demographics, Locations, Product categories

   Key Features:
   - Batch-level and table-level load duration tracking
   - Structured logging using PRINT statements
   - TRY/CATCH error handling for operational visibility
   - Full reload strategy suitable for early-stage pipelines

   Execution Pattern:
   - Truncate Bronze tables
   - Load CSV files via BULK INSERT
   - Capture execution timing per table and per batch

   Notes:
   - Designed as a foundational ingestion step
   - Downstream Silver layer applies cleansing and transformations
   - File paths can be parameterized for production use

   Author: Bernard Mutambo
   ============================================================ */


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================================================';
		PRINT 'Loading the Bronze Layer';
		PRINT '=================================================================================';

		PRINT '=================================================================================';
		PRINT 'LOADING CRM TABLES';
		PRINT '=================================================================================';
		--FIRST crm
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'


		--second crm
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'

		---third crm
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'


		PRINT '=================================================================================';
		PRINT 'LOADING ERP TABLES';
		PRINT '=================================================================================';
		---ERP1
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'


		---ERP2
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'


		---ERP3
		SET @start_time = GETDATE()
		PRINT '>> Trancating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Ben\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load duration: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>----------------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '=================================================================================';
		PRINT '>>LOADING BRONZE LAYER COMPLETE'
		PRINT '>>Total Load duration: ' + CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=================================================================================';


	END TRY
	BEGIN CATCH
		PRINT '=================================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================================================';
	END CATCH
END;
