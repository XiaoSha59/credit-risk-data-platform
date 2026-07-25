import psycopg2
import json
import time
import sys

# PostgreSQL Source Database Connection Settings
DB_CONFIG = {
    "dbname": "credit_risk_dw",
    "user": "creditrisk",
    "password": "creditrisk123",
    "host": "localhost",
    "port": 5432
}

def produce_cdc_source_transactions():
    print("[*] Connecting to PostgreSQL Source Database (credit_risk_dw)...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        print("[+] Successfully connected to PostgreSQL Source DB!")
        
        # 1. Ensure Table Exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.fact_loans (
                loan_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50) NOT NULL,
                loan_amount NUMERIC(15, 2) NOT NULL,
                status VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        print("\n============================================================")
        print("[+] STEP 1: Executing INSERT Operation (CDC 'c' event)...")
        print("============================================================")
        loan_id = f"LOAN-CDC-{int(time.time())}"
        cust_id = "CUST-9901"
        amount = 45000000.00
        
        insert_sql = """
            INSERT INTO public.fact_loans (loan_id, customer_id, loan_amount, status)
            VALUES (%s, %s, %s, 'APPROVED');
        """
        cursor.execute(insert_sql, (loan_id, cust_id, amount))
        print(f"[+] INSERT SUCCESS -> loan_id: {loan_id}, customer_id: {cust_id}, amount: {amount:,.2f} VND, status: APPROVED")
        
        time.sleep(2)
        
        print("\n============================================================")
        print("[+] STEP 2: Executing UPDATE Operation (CDC 'u' event)...")
        print("============================================================")
        update_sql = """
            UPDATE public.fact_loans 
            SET loan_amount = %s, status = 'DISBURSED'
            WHERE loan_id = %s;
        """
        new_amount = 50000000.00
        cursor.execute(update_sql, (new_amount, loan_id))
        print(f"[+] UPDATE SUCCESS -> loan_id: {loan_id}, new_amount: {new_amount:,.2f} VND, status: DISBURSED")
        
        cursor.close()
        conn.close()
        print("\n[+] Source Database operations completed cleanly!")
        return loan_id
        
    except Exception as e:
        print(f"[-] Error connecting to PostgreSQL: {e}")
        return None

if __name__ == "__main__":
    produce_cdc_source_transactions()
