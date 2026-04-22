--------------------------------------------------
-- Dim_Customer--
------------------------------------------------------
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

 -------------------------------------------------------
---dim_product ___
-------------------------------------------------------
create view gold.dim_products as
select
    row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.cat_id as category_id,
    pn.prd_nm as product_name,
    pn.prd_cost as product_cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as product_start_date,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance as maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is Null; -- Filter out Historical Data
 -------------------------------------------------------
---fact_sales
-------------------------------------------------------
Create View gold.fact_sales  as
select
    sd.sls_ord_num as order_number,
    pr.product_key,
    cust.customer_id,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity  as quantity,
    sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customer as cust
on sd.sls_cust_id = cust.customer_id;



select *
from gold.dim_products;