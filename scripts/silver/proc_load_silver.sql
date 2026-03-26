/*
====================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
====================================================================================
Script Purpose:
    This stored procedure performs ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
    - Truncates silver tables.
    - Insert transformed and cleansed data from bronze to silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
====================================================================================
*/


Create or Alter Procedure silver.load_silver as
Begin
	Declare @start_time DateTime, @end_time DateTime, @batch_start_time DateTime, @batch_end_time Datetime
	Begin Try
		set @batch_start_time = GETDATE()
		Print '======================================';   -- Print are used to make data more readable
		Print 'Loading Silver Layer';
		Print '======================================';
	
		Print '-----------------------------------------';
		Print 'Loading CRM tables';
		Print '-----------------------------------------';

		-- Loading silver.crm_cust_info
		Set @start_time = getdate();
		Print '>> truncating table: silver.crm_cust_info';
		Truncate table silver.crm_cust_info;
		print '>> Inserting data into: silver.crm_cust_info';
		Insert into silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date)

		Select
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname)as cst_lastname,
		case 
			when Upper (Trim(cst_material_status)) = 'M' then 'Married'
			when upper(trim(cst_material_status)) = 'S' then 'Single'
			else 'n/a'
			end cst_material_status,
		case 
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			else 'n/a'
			end cst_gndr,
		cst_create_date

		from (
		select *,
		row_number() over(partition by cst_id order by cst_create_date desc) last_flag
		from bronze.crm_cust_info
		where cst_id is not null
		)t where last_flag = 1;
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';

-- Loading second table

		set @start_time = GetDate();
		Print '>> truncating table: silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
		print '>> Inserting data into: silver.crm_prd_info';
		Insert into silver.crm_prd_info(

		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		select 
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') cat_id,
		substring(prd_key, 7, len(prd_key)) prd_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost, 
		case upper(trim(prd_line))
			when 'M' then 'Mounatin'
			when 'R' then 'Road'
			when 'S' then  'Other Sales'
			when 'T' then 'Touring'
			else 'n/a'

		end prd_line,
		cast(prd_start_dt as date) as prd_start_date,
		cast(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date) prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';



-- Loading third table
		set @start_time = GetDate();
		Print '>> truncating table: silver.crm_sales_details';
		Truncate table silver.crm_sales_details;
		print '>> Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (

			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case 
			when sls_order_dt =0 or len(sls_order_dt) != 8 then Null
			else cast(cast(sls_order_dt as varchar)as date) -- to convert integer into date, we need to convert into varchar and then date

		end sls_order_dt,
		case 
			when sls_ship_dt =0 or len(sls_ship_dt) != 8 then Null
			else cast(cast(sls_ship_dt as varchar)as date) -- to convert integer into date, we need to convert into varchar and then date

		end sls_ship_dt,
		case 
			when sls_due_dt =0 or len(sls_due_dt) != 8 then Null
			else cast(cast(sls_due_dt as varchar)as date) -- to convert integer into date, we need to convert into varchar and then date

		end sls_due_dt,
		case 
				when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0
			then sls_sales / nullif(sls_quantity, 0)
			else sls_price
		end sls_price
		from bronze.crm_sales_details;
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';

-- loading 4th table
		set @start_time = GetDate();
		Print '>> truncating table: silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12;
		print '>> Inserting data into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (cid, bdgate, gen)
		select 
		case 
			when cid like 'NAS%' then substring(cid, 4, len(cid))
			else cid

		end as cid,

		case when bdgate > getdate() then null
		else bdgate
		end bdgate,
		case 
			when upper(trim(gen)) in ('F', 'FEMALE') then 'Female' 
			when upper(trim(gen)) in ('M', 'MALE') then 'Male' 
			else 'n/a'
		end gen
		from bronze.erp_cust_az12;
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';

-- loading 5th table
		set @start_time = GetDate();
		Print '>> truncating table: silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101;
		print '>> Inserting data into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (cid, cntry)

		select distinct
		replace(cid, '-', '') cid,
		case
			when Trim(cntry) = 'DE'  then 'Germany'
			when Trim(cntry) = 'US'or cntry = 'USA' then 'United States'
			when Trim(cntry) = '' or cntry is null then 'n/a'
			else Trim(cntry)
		end
		from bronze.erp_loc_a101;
		set @end_time = GETDATE();
		Print '>> Load Duration: '  + cast(Datediff(second, @start_time, @end_time)as NVARCHAR)  +  'seconds';
		Print '>> ---------------------------';


-- loading 6th table
		set @start_time = GetDate();
		Print '>> truncating table: silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting data into: silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenacne)

		select
		id,
		cat,
		subcat,
		maintenacne
		from bronze.erp_px_cat_g1v2
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
