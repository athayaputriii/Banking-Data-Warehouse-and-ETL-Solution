import pandas as pd
import numpy as np
from datetime import datetime
from config.database import get_db_connection, get_dwh_connection

def safe_date_convert(date_val):
    """Convert date safely, handle semua format tanggal"""
    if pd.isna(date_val) or date_val == '':
        return None
    
    try:
        # Jika sudah datetime object atau Timestamp
        if isinstance(date_val, (pd.Timestamp, datetime)):
            return date_val
        
        # Jika string, handle berbagai format
        if isinstance(date_val, str):
            # Format dari CSV: "21-01-2024 14:00:00" (DD-MM-YYYY)
            if '-' in date_val and len(date_val.split('-')[0]) == 2:
                try:
                    return datetime.strptime(date_val, '%d-%m-%Y %H:%M:%S')
                except ValueError:
                    pass
            
            # Format dari Database: "2024-01-17 09:10:00" (YYYY-MM-DD)
            try:
                return datetime.strptime(date_val, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                pass
            
            # Coba format lainnya
            for fmt in ['%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S']:
                try:
                    return datetime.strptime(date_val, fmt)
                except ValueError:
                    continue
        
        # Fallback: pandas to_datetime
        return pd.to_datetime(date_val, errors='coerce')
    
    except Exception as e:
        print(f"Date conversion error for '{date_val}': {e}")
        return None

def extract_transaction_sources():
    """Extract data dari 3 sumber berbeda"""
    
    print("Reading Excel file...")
    df_excel = pd.read_excel('data_sources/transaction_excel.xlsx')
    
    print("Reading CSV file...")  
    df_csv = pd.read_csv('data_sources/transaction_csv.csv')
    
    print("Reading database...")
    with get_db_connection() as conn:
        query = "SELECT * FROM transaction_db"
        df_db = pd.read_sql(query, conn)
    
    return df_excel, df_csv, df_db

def transform_fact_transaction():
    """Gabungkan dan transformasi data transaksi dengan handling date"""
    print("Combining transaction data...")
    
    df_excel, df_csv, df_db = extract_transaction_sources()
    
    # Debug: Show original dates from each source
    print("\n📅 DEBUG DATE FORMATS:")
    print(f"Excel sample date: {df_excel['transaction_date'].iloc[0]} (type: {type(df_excel['transaction_date'].iloc[0])})")
    print(f"CSV sample date: {df_csv['transaction_date'].iloc[0]} (type: {type(df_csv['transaction_date'].iloc[0])})")
    print(f"DB sample date: {df_db['transaction_date'].iloc[0]} (type: {type(df_db['transaction_date'].iloc[0])})")
    
    # Gabungkan semua data
    df_combined = pd.concat([df_excel, df_csv, df_db], ignore_index=True)
    
    # Hapus duplikat berdasarkan TransactionID
    df_clean = df_combined.drop_duplicates(subset=['transaction_id'])
    
    # PascalCase column names
    df_clean.columns = [
        'TransactionID', 'AccountID', 'TransactionDate', 
        'Amount', 'TransactionType', 'BranchID'
    ]
    
    # FIX: Handle TransactionDate conversion
    print("\nConverting transaction dates...")
    df_clean['TransactionDate_Converted'] = df_clean['TransactionDate'].apply(safe_date_convert)
    
    # Debug converted dates
    print("Sample dates after conversion:")
    for i, (orig, conv) in enumerate(zip(df_clean['TransactionDate'].head(5), df_clean['TransactionDate_Converted'].head(5))):
        print(f"  {orig} → {conv}")
    
    # Drop rows dengan tanggal invalid
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['TransactionDate_Converted'])
    final_count = len(df_clean)
    
    if initial_count != final_count:
        print(f"Removed {initial_count - final_count} records with invalid dates")
    
    # Replace original column with converted
    df_clean['TransactionDate'] = df_clean['TransactionDate_Converted']
    df_clean = df_clean.drop('TransactionDate_Converted', axis=1)
    
    # Convert tipe data
    df_clean['TransactionID'] = df_clean['TransactionID'].astype(int)
    df_clean['AccountID'] = df_clean['AccountID'].astype(int)
    df_clean['Amount'] = df_clean['Amount'].astype(float)
    df_clean['BranchID'] = df_clean['BranchID'].astype(int)
    
    print(f"Combined transactions: {len(df_clean)} records")
    
    return df_clean

def load_fact_transaction():
    """Load data ke FactTransaction dengan error handling"""
    print("Loading FactTransaction...")
    
    df_fact = transform_fact_transaction()
    
    with get_dwh_connection() as conn:
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM FactTransaction")
        
        # Insert new data dengan prepared statement
        success_count = 0
        error_count = 0
        
        for index, row in df_fact.iterrows():
            try:
                cursor.execute(
                    """INSERT INTO FactTransaction 
                    (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID) 
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    int(row['TransactionID']), 
                    int(row['AccountID']), 
                    row['TransactionDate'],
                    float(row['Amount']), 
                    str(row['TransactionType']), 
                    int(row['BranchID'])
                )
                success_count += 1
            except Exception as e:
                print(f"Error inserting TransactionID {row['TransactionID']}: {e}")
                error_count += 1
                # Debug detail untuk row yang error
                print(f"   Problematic data: {dict(row)}")
                continue
        
        conn.commit()
    
    print(f"FactTransaction completed: {success_count} successful, {error_count} failed")

if __name__ == "__main__":
    load_fact_transaction()