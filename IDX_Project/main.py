from etl_scripts.etl_dimensions import run_all_dimensions
from etl_scripts.etl_fact import load_fact_transaction

def main():
    print("🚀 STARTING ID/X PARTNERS ETL PROCESS...")
    
    # Step 1: Run Dimensions terlebih dahulu
    run_all_dimensions()
    
    # Step 2: Run Fact tables (setelah dimensions)
    load_fact_transaction()
    
    print("🎉 ALL ETL PROCESSES COMPLETED SUCCESSFULLY!")
    print("📊 Data Warehouse ready for analysis!")

if __name__ == "__main__":
    main()