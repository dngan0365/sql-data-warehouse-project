CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_date DATETIME, @end_date DATETIME;
    BEGIN TRY
        PRINT '===========================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '============================';
        PRINT '----------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @start_date = GETDATE();
        PRINT '----------------------------';
        PRINT 'Loading PRD Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_date = GETDATE();
        PRINT '>>> Load Duration: ' + CAST(DATEDIFF(second, @start_date, @end_date) AS NVARCHAR ) + 'second';
        PRINT '----------------------------';
        PRINT 'Loading Sales Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        PRINT '----------------------------';
        PRINT 'Loading CUST Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        PRINT '----------------------------';
        PRINT 'Loading LOC Tables';
        PRINT '----------------------------';

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        PRINT '----------------------------';
        PRINT 'Loading cust Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        PRINT '----------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\mt200\OneDrive\Desktop\AI_egineering\FSDS\DataEngineer\First_Project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCIRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message' + ERROR_MESSAGE();
    END CATCH

END;

EXECUTE bronze.load_bronze;