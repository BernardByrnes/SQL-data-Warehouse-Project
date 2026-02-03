/* ============================================================
   Layer: Bronze (Raw Ingestion)
   Source Systems: CRM & ERP

   Description:
   This script creates raw ingestion tables in the Bronze layer
   for CRM and ERP source systems. The tables store source data
   in its original structure with minimal transformation.

   Data Characteristics:
   - Raw, source-aligned schemas
   - No business logic applied
   - Designed for traceability and reprocessing
   - Acts as the foundation for Silver-layer transformations

   Source Domains:
   - CRM: Customers, Products, Sales
   - ERP: Customer Demographics, Locations, Product Categories

   Purpose:
   To provide a reliable and auditable landing zone for
   downstream ETL processes in the data warehouse.

   Technologies:
   - SQL Server
   - T-SQL

   Author: Bernard Mutambo
   ============================================================ */


--CRM
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info (
    prd_id    INT,
    prd_key    NVARCHAR(50),
    prd_nm    NVARCHAR(50),
    prd_cost    INT,
    prd_line    NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

IF OBJECT_ID ('bronze.crm_sales_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_info
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

--ERP

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
   cid NVARCHAR(50),
   cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12 (
   cid NVARCHAR(50),
   bdate DATE,
   gen NVARCHAR(50)
);


IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2 (
   id NVARCHAR(50),
   cat NVARCHAR(50),
   subcat NVARCHAR(50),
   maintenance NVARCHAR(50)
);
