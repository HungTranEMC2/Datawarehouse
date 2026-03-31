Install mysql;
load mysql;


-- Replace with your actual credentials
ATTACH 'host=localhost user=root password=12345678 database=bronze'
AS bronze(TYPE mysql);

