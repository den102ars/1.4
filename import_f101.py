import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

def log_event(process_name, status, rows_loaded=0, message=""):
    """Логирует событие в таблицу logs.etl_log"""
    try:
        engine = create_engine('postgresql://postgres:root@localhost:5432/bank_etl')
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO logs.etl_log 
                (process_name, start_time, end_time, status, rows_loaded, message)
                VALUES (:process, NOW(), NOW(), :status, :rows, :msg)
            """), {
                'process': process_name,
                'status': status,
                'rows': rows_loaded,
                'msg': message
            })
            conn.commit()
        return True
    except Exception as e:
        print(f"Logging failed: {e}")
        return False

def import_from_csv():
    start_time = datetime.now()
    process_name = "import_f101"
    log_event(process_name, 'STARTED', message='Starting import')
    
    try:
        engine = create_engine('postgresql://postgres:root@localhost:5432/bank_etl')
        
        # 1. Создаем копию таблицы
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS dm.dm_f101_round_f_v2"))
            conn.execute(text("""
                CREATE TABLE dm.dm_f101_round_f_v2 (LIKE dm.dm_f101_round_f INCLUDING ALL)
            """))
            conn.commit()
        
        
        # 2. Читаем CSV
        df = pd.read_csv('f101_report.csv')
        print(f"Read CSV: {len(df)} rows")
        
        # 3. Записываем в БД
        df.to_sql(
            'dm_f101_round_f_v2', 
            engine, 
            schema='dm', 
            if_exists='append', 
            index=False,
            method='multi'
        )
        
        # Успешное завершение
        message = f"Imported {len(df)} rows"
        print(message)
        log_event(process_name, 'COMPLETED', len(df), message)
        return True
        
    except Exception as e:
        error_msg = f"Import failed: {str(e)}"
        print(error_msg)
        log_event(process_name, 'ERROR', message=error_msg)
        return False

if __name__ == "__main__":
    if import_from_csv():
        print("Success!")
    else:
        print("Failed!")