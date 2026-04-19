#!/bin/bash

set -e

echo "==> Starting CSV load from /data/"

export PGOPTIONS="-c datestyle=ISO,MDY"

COLS="id,customer_first_name,customer_last_name,customer_age,customer_email,customer_country,customer_postal_code,customer_pet_type,customer_pet_name,customer_pet_breed,seller_first_name,seller_last_name,seller_email,seller_country,seller_postal_code,product_name,product_category,product_price,product_quantity,sale_date,sale_customer_id,sale_seller_id,sale_product_id,sale_quantity,sale_total_price,store_name,store_location,store_city,store_state,store_country,store_phone,store_email,pet_category,product_weight,product_color,product_size,product_brand,product_material,product_description,product_rating,product_reviews,product_release_date,product_expiry_date,supplier_name,supplier_contact,supplier_email,supplier_phone,supplier_address,supplier_city,supplier_country"

# Собираем файлы через bash glob (корректно работает с пробелами)
shopt -s nullglob
csv_files=(/data/MOCK_DATA*.csv)
shopt -u nullglob

IFS=$'\n' csv_files=($(printf '%s\n' "${csv_files[@]}" | sort -V))
unset IFS

echo "==> Found ${#csv_files[@]} CSV files"

loaded_total=0
files_loaded=0

for csv_file in "${csv_files[@]}"; do
    filename=$(basename "$csv_file")
    echo "--> Loading $filename"

    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
\copy raw.mock_data ($COLS) FROM '$csv_file' WITH (FORMAT csv, HEADER true, NULL '');
UPDATE raw.mock_data SET loaded_from_file = '$filename' WHERE loaded_from_file IS NULL;
SQL

    rows=$(psql -t -A --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
           -c "SELECT COUNT(*) FROM raw.mock_data WHERE loaded_from_file = '$filename';")
    echo "    Loaded $rows rows from $filename"
    loaded_total=$((loaded_total + rows))
    files_loaded=$((files_loaded + 1))
done

echo "==> Total loaded: $loaded_total rows from $files_loaded files"

echo "==> Verifying load:"
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
SELECT
    COUNT(*)                           AS total_rows,
    COUNT(DISTINCT id)                 AS unique_source_ids,
    COUNT(DISTINCT loaded_from_file)   AS files_loaded,
    MIN(sale_date)                     AS earliest_sale,
    MAX(sale_date)                     AS latest_sale
FROM raw.mock_data;
SQL

if [ "$loaded_total" -ne 10000 ]; then
    echo "!!! WARNING: expected 10000 rows, got $loaded_total"
fi
if [ "$files_loaded" -ne 10 ]; then
    echo "!!! WARNING: expected 10 files, got $files_loaded"
fi

echo "==> CSV load completed successfully"