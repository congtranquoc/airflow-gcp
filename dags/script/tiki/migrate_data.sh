BUCKET="de-airflow-bucket"
FILE="/home/quoccong-workspace/airflow_project/datas/tiki_export.json"

gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" -m cp "$FILE" gs://"$BUCKET"
