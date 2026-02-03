/* ============================================================
   Project: SQL Data Warehouse Project
   Description:
   This script initializes the DataWarehouse environment by:
   - Dropping and recreating the database (if it exists)
   - Creating schemas following the Medallion Architecture
     (Bronze, Silver, Gold)

   Purpose:
   To provide a clean, repeatable setup for building and
   testing an end-to-end SQL Server data warehouse with
   ETL pipelines and analytics layers.

   Technologies:
   - SQL Server

   Author: Bernard Mutambo
   Repository: https://github.com/BernardByrnes/SQL-data-Warehouse-Project
   ============================================================ */

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
