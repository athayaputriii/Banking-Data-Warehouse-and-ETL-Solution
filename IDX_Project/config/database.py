import pyodbc
import pandas as pd

def get_db_connection():
    """Koneksi ke SQL Server database sample"""
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=ATHAYAPUTRI\\SQLSERVER;'  # ← PAKAI SERVER NAME INI
        'DATABASE=sample;'
        'Trusted_Connection=yes;'
    )
    print("Connected to SAMPLE database")
    return conn

def get_dwh_connection():
    """Koneksi ke Data Warehouse DWH"""
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=ATHAYAPUTRI\\SQLSERVER;'  # ← PAKAI SERVER NAME INI
        'DATABASE=DWH;'
        'Trusted_Connection=yes;'
    )
    print("Connected to DWH database")
    return conn

# Test function
def test_connections():
    """Test both connections"""
    try:
        conn_sample = get_db_connection()
        conn_sample.close()
        
        conn_dwh = get_dwh_connection()
        conn_dwh.close()
        
        print("🎉 All connections successful!")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_connections()