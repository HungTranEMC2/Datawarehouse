Install mysql;
load mysql;


-- Replace with your actual credentials
ATTACH 'host=localhost user=root password=12345678 database=bronze'
AS bronze(TYPE mysql);

use bronze;


ATTACH 'host=localhost user=root password=12345678 database=silver'
AS silver(TYPE mysql);

use silver;

ATTACH 'host=localhost user=root password=12345678 database=gold'
AS gold(TYPE mysql);

use gold;
