#!/bin/bash

set -e

docker exec retail-dbt bash -c "
cd /usr/app/retail_sales_lakehouse &&

dbt deps &&
dbt build
"