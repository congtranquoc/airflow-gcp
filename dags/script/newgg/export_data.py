import sqlalchemy as db
import pandas as pd
import os
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

username = os.getenv('username')
password = os.getenv('password')

encoded_password = urllib.parse.quote_plus(password)
engine = db.create_engine(f"mysql+mysqlconnector://{username}:{encoded_password}@localhost:3306/newegg_graphic_cards")

connection = engine.connect()
query = "SELECT * FROM products"
query_df = pd.read_sql_query(query, engine)

# Đổi dấu phân tách sang ';'
csv_file_path = "/home/quoccong-workspace/airflow_project/datas/newedge-graphic-cards.csv"
query_df.to_csv(csv_file_path, index=False, sep=';')

connection.close()
