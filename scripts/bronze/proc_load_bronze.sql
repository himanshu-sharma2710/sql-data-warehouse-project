/*
====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
====================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
====================================================================================
*/


Create or Alter Procedure bronze.load_bronze as
Begin
	Declare @start_time DateTime, @end_time DateTime, @batch_start_time DateTime, @batch_end_time Datetime
	Begin Try
		set @batch_start_time = GETDATE()
		Print '======================================';   -- Print are used to make data more readable
		Print 'Loading Bronze Layer';
		Print '======================================';
	
		Print '-----------------------------------------';
		Print 'Loading CRM tables';
		Print '-----------------------------------------';

		set @start_time = GETDATE();
		Print '>> Truncating table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
	
		Print '>> Inserting data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			
		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';



		-- Load second table 
		set @start_time = GetDAte();
		Print '>> Truncating table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
	
		Print '>> Inserting data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'

		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';


		-- Load third table
		
		set @start_time = getDate();
		Print '>> Truncating table: bronze.crm_sales_details';

		Truncate Table bronze.crm_sales_details; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
	
		Print '>> Inserting data Into: bronze.crm_sales_details';
	
		Bulk Insert bronze.crm_sales_details
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'

		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';


		-- Load fourth Table 

		set @start_time = GETDATE();
		Print '-----------------------------------------';
		Print 'Loading ERP tables';
		Print '-----------------------------------------';

		Print '>> Truncating table: bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
		Print '>> Inserting data Into: bronze.erp_loc_a101';
	
		Bulk Insert bronze.erp_loc_a101
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'

		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';


		-- Load Fifth table
		set @start_time = getDate();

		Print '>> Truncating table: bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
		Print '>> Inserting data Into: bronze.erp_cust_az12';

		Bulk Insert bronze.erp_cust_az12
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'

		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';

		-- Load sixth table
		
		set @start_time = GETDATE();
		Print '>> Truncating table: bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2; -- truncate will empty the table and the reload, eliminating risk of duplicacy

		-- Use bulk insert to insert data into table from czv or txt files
		Print '>> Inserting data Into: bronze.erp_px_cat_g1v2';
	
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'C:\Users\HIMANSHU SHARMA\OneDrive\Documents\SQL Server Management Studio 22\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'

		with (

		Firstrow = 2,  -- tell sql from which row the actual data starts
		FieldTerminator = ',', -- tell sql the data in file is separated by what (.,.,'',# etc)
		Tablock -- to optimize performance of load

		);
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';

		set @batch_end_time = GETDATE();
		Print '====================================='
		Print 'Loading Bronze Layer in completed'
		Print '  - Total Load Duration : ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		Print '======================================'
	End try
	Begin Catch
		Print '==============================================='
		Print 'Error occured during loading bronze layer'		
		Print 'Error Message' + Error_message();
		Print 'Error Message' + cast(Error_number() as NVARCHAR);
		Print 'Error Message' + Cast(Error_State() as NVARCHAR);
		Print '==============================================='
	End Catch
End
