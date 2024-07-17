gsutil -m rm -r "${BUCKET}/*"

for FILE in big_queries/*.sql 
do
  START_TIME=$(date +%s)
  TABLE=$(basename $FILE .sql)

  bq query --use_legacy_sql=false --destination_table=${DATASET}.${TABLE} --replace --nouse_cache --nouse_legacy_sql --allow_large_results --noflatten_results --batch --use_cache --format=none "$(cat $FILE)"


  bq extract \
    --destination_format=CSV \
    --compression=GZIP \
    --field_delimiter="," \
    --print_header=true \
    "${DATASET}.${TABLE}" \
    "${BUCKET}/${TABLE}.*.csv.gz"
    
    
  bq rm -f ${DATASET}.${TABLE}

  END_TIME=$(date +%s)
  DURATION=$((END_TIME - START_TIME))
  echo "Exported ${FILE} in ${DURATION} seconds"
done
