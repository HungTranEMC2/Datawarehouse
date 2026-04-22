--create or alter procedure bronze.load_bronze

--insert cust_info.csv----
insert into bronze.bronze.crm_cust_info
select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/cust_info.csv',
                  header = true,
                  delim = ',',
                  skip = 1);


--insert prd_info.csv----
insert into bronze.bronze.crm_prd_info
select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/prd_info.csv',
                 header = true,
                  delim = ',',
                  skip = 1););

--insert sales_details.csv----
insert into bronze.bronze.crm_sales_details
select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/sales_details.csv',
                 header = true,
                  delim = ',',
                  skip = 1);

    --insert CUST_AZ12.csv----
    insert into bronze.bronze.erp_cust_az12
    select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/CUST_AZ12.csv',
                     header = true,
                  delim = ',',
                  skip = 1););

    --insert LOC_A101.csv----
    insert into bronze.bronze.erp_loc_a101
    select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/LOC_A101.csv',
                     header = true,
                  delim = ',',
                  skip = 1););

    --insert PX_CAT_G1V2.csv----
    insert into bronze.bronze.erp_px_cat_g1v2
    select * from read_csv('/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/PX_CAT_G1V2.csv',
                     header = true,
                  delim = ',',
                  skip = 1););




