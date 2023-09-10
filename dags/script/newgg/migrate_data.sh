BUCKET="de-airflow-bucket"
FILE="/home/quoccong-workspace/airflow_project/datas/newedge-graphic-cards.csv"

gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" -m cp "$TEMP" gs://"$BUCKET"
