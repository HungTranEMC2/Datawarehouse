create table bronze.bronze.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

create table bronze.bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
);

create table bronze.bronze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id varchar(50),
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
);

create table bronze.bronze.erp_cust_az12(
CID varchar(50),
BDATE Date,
GEN varchar(50),
);

create table bronze.bronze.erp_loc_a101(
CID varchar(50),
CNTRY VARCHAR(50),
);

create table bronze.bronze.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
);

drop table bronze.bronze.crm_sales_details;