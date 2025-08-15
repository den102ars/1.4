import psycopg2
import csv

def log_event(process, status, rows=0, message=""):
    try:
        conn = psycopg2.connect(
            dbname='bank_etl',
            user='postgres',
            password='root',
            host='localhost',
            port='5432'
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs.etl_log 
                (process_name, start_time, end_time, status, rows_loaded, message)
                VALUES (%s, NOW(), NOW(), %s, %s, %s)
            """, (process, status, rows, message))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Logging failed: {e}")
        return False

def export_to_csv():
    try:
        # Логирование начала
        log_event('export_f101', 'STARTED', 0, 'Starting export')
        
        # Подключение к БД
        conn = psycopg2.connect(
            dbname='bank_etl',
            user='postgres',
            password='root',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()
        
        # Выгрузка данных
        cur.execute("SELECT * FROM dm.dm_f101_round_f")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        
        # Запись в CSV
        with open('f101_report.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)
        
        # Логирование успеха
        log_event('export_f101', 'COMPLETED', len(rows), 'Export successful')
        print(f"Exported {len(rows)} rows to f101_report.csv")
        return True
        
    except Exception as e:
        # Логирование ошибки
        log_event('export_f101', 'ERROR', 0, str(e))
        print(f"Export failed: {e}")
        return False

if __name__ == "__main__":
    export_to_csv()