
import sqlalchemy as db
import pandas as pd
import os
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

username = os.getenv('username')
password = os.getenv('password')

encoded_password = urllib.parse.quote_plus(password)
engine = db.create_engine(f"mysql+mysqlconnector://{username}:{encoded_password}@localhost:3306/newedge_graphic_cards")

# Đường dẫn đến file CSV
csv_file_path = '/home/quoccong-workspace/airflow_project/scripts/newegg/Products.csv'

# Đọc dữ liệu từ CSV
df = pd.read_csv(csv_file_path, delimiter=';')

# Kết nối đến cơ sở dữ liệu
connection = engine.connect()

# Thêm dữ liệu vào bảng products
table_name = 'products'
df.to_sql(table_name, con=connection, if_exists='replace', index=False)

# Đóng kết nối
connection.close()



