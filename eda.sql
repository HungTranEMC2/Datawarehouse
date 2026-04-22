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
where flag_last = 1; -- Select the most recent record per customer
--Check for Null or Duplicate Values
select cst_id,
       count(*)
from silver.silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

select *
from bronze.bronze.crm_cust_info
where cst_id = 29483;


select *
from (
select
    *,
    row_number() over (Partition by cst_id order by cst_create_date Desc) as flag_last
from bronze.bronze.crm_cust_info) as t
where flag_last != 1 and cst_id 29483;
--where cst_id = 29483;

-- Check for unwanted spaces --
-- Expectation: No Result
select cst_lastname
from bronze.bronze.crm_cust_info
where cst_lastname != TRIM(cst_firstname);


--Data Standardization & Consistency--
Select DISTINCT cst_gndr
from bronze.crm_cust_info;

select distinct cst_marital_status
from bronze.crm_cust_info;
--------------------------------------------------
--------------------------------------------------
select trim(prd_nm) as trimmed_prd_nm,
       prd_nm
from bronze.bronze.crm_prd_info;insert into silver.silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
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
where flag_last = 1; -- Select the most recent record per customer
--Check for Null or Duplicate Values
select cst_id,
       count(*)
from silver.silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL;

select *
from bronze.bronze.crm_cust_info
where cst_id = 29483;


select *
from (
select
    *,
    row_number() over (Partition by cst_id order by cst_create_date Desc) as flag_last
from bronze.bronze.crm_cust_info) as t
where flag_last != 1 and cst_id 29483;
--where cst_id = 29483;

-- Check for unwanted spaces --
-- Expectation: No Result
select cst_lastname
from bronze.bronze.crm_cust_info
where cst_lastname != TRIM(cst_firstname);


--Data Standardization & Consistency--
Select DISTINCT cst_gndr
from bronze.crm_cust_info;

select distinct cst_marital_status
from bronze.crm_cust_info;
--------------------------------------------------
--- crm_prd_info table-----
--------------------------------------------------
select
    prd_id,
    prd_key,
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

select prd_nm
from bronze.bronze.crm_prd_info
where prd_nm != TRIM(prd_nm);

--Check for Null or Negative Numbers
--Expectation: No Results
Select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost = Null;

select prd_cost
from bronze.crm_prd_info
where prd_cost is  NULL;


select distinct prd_line
from bronze.bronze.crm_prd_info
order by 1;

--Check for Invalid Date Orders
-- End Date must not be earlier than the start date
Select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;


select prd_id,
       prd_key,
       prd_nm,
       prd_start_dt,
--       prd_end_dt,
    LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) as prd_end_dt_test,
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');

select prd_id,
       count(*)
from silver.silver.crm_prd_info
group by prd_id
having count(*) >1;

select prd_nm
from silver.silver.crm_prd_info
where prd_nm != trim(prd_nm);

select *
from bronze.bronze.crm_sales_details
where sls_ord_num like '%51212%';

select sls_ord_num,
       count(*)
from bronze.bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1;

select sls_ord_num,
       strptime(nullifsls_order_dt, '%Y%m%d') as sls_order_dt,
       sls_ship_dt
from bronze.bronze.crm_sales_details;

select cast(sls_order_dt as int) as sls_order_dt
from bronze.crm_sales_details
where cast(sls_order_dt as int) <= 0;

select *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from bronze.silver.crm_prd_info);

select nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
   or len(cast(sls_order_dt as varchar)) != 8
    or sls_order_dt > 20250101
    or sls_order_dt < 19000101;
---------------------------------------------------------------------
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

select sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0;

select sls_due_dt
from bronze.bronze.crm_sales_details
where sls_due_dt < 19000101 ;

select sls_ord_num,
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


select sls_ord_num,
        sls_sales,
       sls_quantity,
       abs(sls_price)
from bronze.bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price;

select sls_price,
from bronze.bronze.crm_sales_details
where sls_price <=0;

select *,
    case when sls_price is Null then sls_sales/ sls_quantity
        when sls_price <= 0 then abs(sls_price)
    end as sls_price
from bronze.bronze.crm_sales_details
where 1 =1
aND sls_price is Null
OR sls_price <=0;


select CID,
        case when Trim(Upper(GEN)) = 'F' Then 'Female'
        when Trim(Upper(GEN)) = 'M' Then 'Male'
        when Trim(Upper(GEN)) is Null or Trim(Upper(GEN)) = '' then 'unknown'
    else Trim(Upper(Gen))
    end as Gen
from bronze.silver.crm_sales_details;
------------------------------------------------------
select
    CID,
    case when cid like 'NAS%' Then SUBSTRING(cid, 4, len(cid))
    else cid
        end as new_cid,
    BDate,
    case when bdate > current_date() then null
    else bdate
    end as bdate,
    case when Upper(Trim(GEN)) = 'F' Then 'Female'
        when Trim(Upper(GEN)) = 'M' Then 'Male'
        when Trim(Upper(GEN)) is Null or Trim(Upper(GEN)) = '' then 'unknown'
    else Trim(Upper(Gen))
    end as Gen
from bronze.erp_cust_az12;



select
    case when bdate > current_date() then null
    else bdate
    end as bdate
from bronze.erp_cust_az12
where bdate > current_date();

select
    CID,
    case when trim(upper(cntry)) like '%US%' then 'United States'
        when trim(upper(cntry)) like '%DE%' then 'Germany'
        when trim(upper(cntry)) is Null or trim(upper(cntry)) = '' then 'unknown'
    else CNTRY
    end as cntry
from bronze.bronze.erp_loc_a101;


select *
from bronze.silver.erp_loc_a101
where trim(cntry) like '';


select distinct Gen ,
    case when gen is null then 'unknown'
        when upper(trim(gen)) in ('F','FEMALE')  then 'Female'
        when upper(trim(gen)) like 'F%' then 'Female'
        when upper(trim(gen)) in ('M','MALE')  then 'Male'
        when upper(trim(gen)) like 'M%' then 'Male'
    Else 'n/a'
    end as new_Gen
from bronze.erp_cust_az12;

select *
from bronze.bronze.erp_cust_az12
where upper(trim(gen)) like 'M%';


select distinct Gen
from silver.silver.erp_cust_az12;

select distinct cntry, len(cntry)
from silver.silver.erp_loc_a101;

select
    distinct cntry,
    case when trim(upper(cntry)) like '%US%' then 'United States'
        when trim(upper(cntry)) = ''
        when trim(upper(cntry)) like '%DE%' then 'Germany'
        when trim(upper(cntry)) is Null or trim(upper(cntry)) = '' then 'unknown'
    else CNTRY
    end as new_cntry
from bronze.bronze.erp_loc_a101;

select distinct cntry as old_cntry,
       case when TRIM(cntry) = 'DE' Then 'Germany'
            when trim(cntry) in ('USA, US',) THen 'United States'
            when trim(cntry) = '' or trim(cntry) is Null then 'n/a'
        else trim(CNTRY)
        end as new_cntry
from bronze.erp_loc_a101
order by cntry;

select
    distinct cntry,
    case when upper(regexp_replace(trim(cntry), '\s+', ' ', 'g'))  like '%DE%' Then 'Germany'
        when upper(regexp_replace(trim(cntry), '\s+', ' ', 'g')) like '%US%' then 'United States'
        when regexp_replace(cntry, '\s+', '', 'g') = '' or cntry is NULL then 'unknown'
    else cntry
    end as new_cntry,
    len(new_cntry)
from bronze.bronze.erp_loc_a101;

select  distinct cntry,
                 len(cntry),
        case
            when regexp_replace(cntry, '\s+', '', 'g') = '' THEN '2'
        else cntry›
        end as new_cntry
from bronze.bronze.erp_loc_a101
where len(cntry) = 4;

drop table silver.silver.erp_loc_a101;

select distinct trim(cntry),
                len(trim(cntry))
from silver.silver.erp_loc_a101;

select *
from bronze.silver.erp_px_cat_g1v2;

------------------------------------------------------------------------
create view gold.dim_customer as
select
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id as customer_id,
      ci.cst_key as customer_number,
      ci.cst_firstname as first_name,
      ci.cst_lastname as last_name,
    la.cntry as country,
      ci.cst_marital_status as marital_status,
        case when ci.cst_gndr = 'unknown' then ca.gen
           when ci.cst_gndr != ca.gen Then ci.cst_gndr --- CRM is the Master for gender info
        else ca.gen
        end as gender,
       ci.cst_create_date as create_date,
       ca.bdate as birthdate,
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;



select ci.cst_gndr,
       ca.gen,
       case when ci.cst_gndr = 'unknown' then ca.gen
           when ci.cst_gndr != ca.gen Then ci.cst_gndr --- CRM is the Master for gender info
        else ca.gen
        end as new_gen
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid;

select *
from gold.dim_customer;