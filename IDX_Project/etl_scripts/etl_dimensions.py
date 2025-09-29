import pandas as pd
from config.database import get_db_connection, get_dwh_connection

def etl_dim_branch():
    """ETL untuk DimBranch"""
    print("Processing DimBranch...")
    
    # Extract dari source
    with get_db_connection() as conn:
        query = "SELECT branch_id, branch_name, branch_location FROM branch"
        df_branch = pd.read_sql(query, conn)
    
    # Transform (PascalCase column names)
    df_branch.columns = ['BranchID', 'BranchName', 'BranchLocation']
    
    # Load ke DWH
    with get_dwh_connection() as conn:
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM DimBranch")
        
        # Insert new data
        for _, row in df_branch.iterrows():
            cursor.execute(
                "INSERT INTO DimBranch (BranchID, BranchName, BranchLocation) VALUES (?, ?, ?)",
                row['BranchID'], row['BranchName'], row['BranchLocation']
            )
        conn.commit()
    
    print(f"DimBranch completed: {len(df_branch)} records")

def etl_dim_account():
    """ETL untuk DimAccount"""
    print("Processing DimAccount...")
    
    with get_db_connection() as conn:
        query = """
        SELECT account_id, customer_id, account_type, balance, date_opened, status 
        FROM account
        """
        df_account = pd.read_sql(query, conn)
    
    # Transform
    df_account.columns = ['AccountID', 'CustomerID', 'AccountType', 'Balance', 'DateOpened', 'Status']
    
    # Load ke DWH
    with get_dwh_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM DimAccount")
        
        for _, row in df_account.iterrows():
            cursor.execute(
                """INSERT INTO DimAccount 
                (AccountID, CustomerID, AccountType, Balance, DateOpened, Status) 
                VALUES (?, ?, ?, ?, ?, ?)""",
                row['AccountID'], row['CustomerID'], row['AccountType'], 
                row['Balance'], row['DateOpened'], row['Status']
            )
        conn.commit()
    
    print(f"DimAccount completed: {len(df_account)} records")

def etl_dim_customer():
    """ETL untuk DimCustomer (paling kompleks)"""
    print("Processing DimCustomer...")
    
    with get_db_connection() as conn:
        query = """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.address,
            city.city_name,
            state.state_name,
            c.age,
            c.gender,
            c.email
        FROM customer c
        LEFT JOIN city ON c.city_id = city.city_id
        LEFT JOIN state ON city.state_id = state.state_id
        """
        df_customer = pd.read_sql(query, conn)
    
    # TRANSFORM: Uppercase untuk kolom tertentu
    df_customer['customer_name'] = df_customer['customer_name'].str.upper()
    df_customer['address'] = df_customer['address'].str.upper()
    df_customer['city_name'] = df_customer['city_name'].str.upper()
    df_customer['state_name'] = df_customer['state_name'].str.upper()
    df_customer['gender'] = df_customer['gender'].str.upper()
    
    # PascalCase column names
    df_customer.columns = [
        'CustomerID', 'CustomerName', 'Address', 'CityName', 
        'StateName', 'Age', 'Gender', 'Email'
    ]
    
    # Load ke DWH
    with get_dwh_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM DimCustomer")
        
        for _, row in df_customer.iterrows():
            cursor.execute(
                """INSERT INTO DimCustomer 
                (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                row['CustomerID'], row['CustomerName'], row['Address'], 
                row['CityName'], row['StateName'], row['Age'], 
                row['Gender'], row['Email']
            )
        conn.commit()
    
    print(f"DimCustomer completed: {len(df_customer)} records")

def run_all_dimensions():
    """Jalankan semua ETL dimension"""
    etl_dim_branch()
    etl_dim_account()
    etl_dim_customer()
    print("🎉 ALL DIMENSION TABELS COMPLETED!")

if __name__ == "__main__":
    run_all_dimensions()