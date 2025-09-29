import pandas as pd
import pyodbc
from datetime import datetime

def debug_transaction_dates():
    """Cek data tanggal dari ketiga sumber"""
    
    print("🔍 DEBUGGING TRANSACTION DATES...")
    print("=" * 60)
    
    # 1. Check Excel file dates
    print("📊 EXCEL FILE DATES:")
    df_excel = pd.read_excel('data_sources/transaction_excel.xlsx')
    print(f"Excel records: {len(df_excel)}")
    print("Transaction dates in Excel:")
    for date in df_excel['transaction_date'].head(10):
        print(f"  {date} (type: {type(date)})")
    print()
    
    # 2. Check CSV file dates  
    print("📄 CSV FILE DATES:")
    df_csv = pd.read_csv('data_sources/transaction_csv.csv')
    print(f"CSV records: {len(df_csv)}")
    print("Transaction dates in CSV:")
    for date in df_csv['transaction_date'].head(10):
        print(f"  {date} (type: {type(date)})")
    print()
    
    # 3. Check Database dates
    print("🗄️ DATABASE DATES:")
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=ATHAYAPUTRI\\SQLSERVER;'
        'DATABASE=sample;'
        'Trusted_Connection=yes;'
    )
    df_db = pd.read_sql("SELECT * FROM transaction_db", conn)
    print(f"Database records: {len(df_db)}")
    print("Transaction dates in Database:")
    for date in df_db['transaction_date'].head(10):
        print(f"  {date} (type: {type(date)})")
    
    conn.close()

if __name__ == "__main__":
    debug_transaction_dates()