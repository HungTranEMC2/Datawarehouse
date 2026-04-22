create table silver.silver.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE,
dwh_create_date TIMESTAMP default current_timestamp
);

create table silver.silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date TIMESTAMP default current_timestamp
);

create table silver.silver.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id varchar(50),
sls_order_dt varchar(50),
sls_ship_dt varchar(50),
sls_due_dt varchar(50),
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date TIMESTAMP default current_timestamp
);

create table silver.silver.erp_cust_az12(
CID varchar(50),
BDATE Date,
GEN varchar(50),
dwh_create_date TIMESTAMP default current_timestamp
);

create table silver.silver.erp_loc_a101(
CID varchar(50),
CNTRY VARCHAR(50),
dwh_create_date TIMESTAMP default current_timestamp
);

create table silver.silver.erp_px_cat_g1v2(
ID varchar(50),
CAT varchar(50),
SUBCAT varchar(50),
MAINTENANCE varchar(50),
dwh_create_date TIMESTAMP default current_timestamp
);


