-- Change the Delimiter
DELIMITER //

-- Drop procedure if exists
drop procedure if exists bronze.load_bronze //

-- Create the stored procedure
Create  Procedure bronze.load_bronze()
Begin
    -- Load data to crm_cust_info---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/cust_info.csv'
    into table bronze.crm_cust_info
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;

    -- Load data to crm_prd_info---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/prd_info.csv'
    into table bronze.crm_prd_info
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;

    -- Load data to crm_sales_details---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_crm/sales_details.csv'
    into table bronze.crm_sales_details
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;

    -- Load data to erp_cust_az12---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/CUST_AZ12.csv'
    into table bronze.erp_cust_az12
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;

    -- Load data to erp_loc_a101---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/LOC_A101.csv'
    into table bronze.erp_loc_a101
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;

    -- Load data to erp_px_cat_g1v2---
    load data local infile '/Users/alexfile/PycharmProjects/Datawarehouse/datasets/source_erp/PX_CAT_G1V2.csv'
    into table bronze.erp_px_cat_g1v2
    Fields terminated by ','
    enclosed by '"'
    lines terminated by '\n'
    Ignore 1 rows;
END //

-- Reset the delimiter back to ;
DELIMITER ;
