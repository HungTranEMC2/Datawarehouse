--create or alter procedure bronze.load_bronze

--insert cust_info.csv----
TRUNCATE TABLE silver.silver.crm_cust_info;
insert into silver.silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
select  cst_id,
    cst_key,
    Trim (cst_firstname) as cst_firstname,
    Trim(cst_lastname) as cst_lastname,
    case when cst_marital_status = 'M' THEN 'Married'
        when cst_marital_status = 'S' THEN 'Single'
        else 'unknown'
    end as cst_marital_status, --normalize marital status values to readable format
    case when Upper(trim(cst_gndr)) = 'F' THEN 'Female'
        when Upper(trim(cst_gndr)) = 'M'  THEN 'Male'
        else 'unknown'
    end as cst_gndr, -- normalize gender values to readable format
    cst_create_date
from (
    select
        *,
        row_number() over (Partition by cst_id order by cst_create_date desc) as flag_last
     from bronze.bronze.crm_cust_info
     where cst_id is not null) as t
where flag_last = 1;


--insert prd_info.csv----
truncate table silver.silver.crm_prd_info;
insert into silver.silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost ,prd_line, prd_start_dt, prd_end_dt)
select
    prd_id,
--    prd_key,
       replace(substring(prd_key, 1, 5), '-','_') as cat_id,
       substring(prd_key,7,len(prd_key)) as prd_key,
       prd_nm,
       prd_cost,
       case upper(trim(prd_line))
           when  'M' THEN 'Mountain'
           when  'R' Then 'Road'
            when  'S' THEN 'Other Sales'
            when 'T' THEN 'Touring'
        else 'n/a'
        end prd_line,
       cast (prd_start_dt as Date) as prd_start_dt,
        cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as Date) as prd_end_dt，
from bronze.bronze.crm_prd_info;

--insert sales_details.csv----
truncate table silver.silver.crm_sales_details;
insert into silver.silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
select sls_ord_num,
       sls_prd_key,
       sls_cust_id,
       case when sls_order_dt <= 0 or len(cast(sls_order_dt as varchar)) != 8 or sls_order_dt > 20250101 or sls_order_dt < 19000101 then NULL
        else strptime(cast(sls_order_dt as varchar(50)), '%Y%m%d')
        end as sls_order_dt,
       strptime(cast(sls_ship_dt as varchar(50)), '%Y%m%d') as sls_ship_dt,
       strptime(cast(sls_due_dt as varchar(50)), '%Y%m%d') as sls_due_dt,
       case when sls_sales is Null Then sls_price * sls_quantity
            when sls_sales = 0 then sls_price * sls_quantity
            when sls_sales <= 0 then sls_price * sls_quantity
            when abs(sls_sales) != abs(sls_price) * sls_quantity then abs(sls_price) * sls_quantity
        else sls_sales
        end as sls_sales,
    case when sls_quantity is Null or sls_quantity <=0 then sls_sales/ sls_price
        else sls_quantity
        end as sls_quantity,
    case when sls_price is Null OR sls_price <= 0 OR sls_price = 0 then sls_sales/ sls_quantity
        else sls_price
        end as sls_price,
from bronze.bronze.crm_sales_details;

    --insert CUST_AZ12.csv----
truncate table silver.silver.erp_cust_az12;
insert into silver.silver.erp_cust_az12(cid, bdate, gen)
select
    case when cid like 'NAS%' Then SUBSTRING(cid, 4, len(cid))
    else cid
        end as cid,
    case when bdate > current_date() then null
    else bdate
    end as bdate,
    case when gen is null then 'n/a'
        when upper(trim(gen)) in ('F','FEMALE')  then 'Female'
        when upper(trim(gen)) like 'F%' then 'Female'
        when upper(trim(gen)) in ('M','MALE')  then 'Male'
        when upper(trim(gen)) like 'M%' then 'Male'
    Else 'n/a'
    end as Gen
from bronze.erp_cust_az12;

    --insert LOC_A101.csv----
truncate table silver.silver.erp_loc_a101;
insert into silver.silver.erp_loc_a101(CID,CNTRY)
    select
    replace(CID,'-','') as CID,
    case when trim(cntry) like '%DE%' Then 'Germany'
         when trim(cntry) = 'Germany' Then 'Germany'
        when trim(cntry) like '%US%' then 'United States'
        when trim(CNTRY) = 'United States' Then 'United States'
        when regexp_replace(cntry, '\s+', '', 'g') = ''or cntry is NULL then 'unknown'
    else cntry
    end as new_cntry
from bronze.bronze.erp_loc_a101;

    --insert PX_CAT_G1V2.csv----
truncate table silver.silver.erp_px_cat_g1v2;
    insert into silver.silver.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)
    select *
    from bronze.bronze.erp_px_cat_g1v2;




