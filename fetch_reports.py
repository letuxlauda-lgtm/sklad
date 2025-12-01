import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация БД
DB_HOST = os.getenv("DB_HOST", "49.13.142.186")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "datavodolij")
DB_USER = os.getenv("DB_USER", "dataanalyst")
DB_PASSWORD = os.getenv("DB_PASSWORD", "))vodoliJuser2025")

# Список файлов для извлечения
REPORT_FILES = [
    "otchet_ruslan.txt",
    "otchet_dmutro.txt",
    "otchet_general.txt",
    "otchet_igor.txt",
    "otchet_inki.txt",
    "service_glub_analitik.txt"
]

def get_connection():
    """Создает подключение к БД PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return None

def fetch_latest_report(file_name):
    """Извлекает самый свежий отчет по названию файла"""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        
        query = """
            SELECT filename, content, created_at 
            FROM automation_txt_files 
            WHERE filename = %s 
            ORDER BY created_at DESC 
            LIMIT 1
        """
        
        cur.execute(query, (file_name,))
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return result
    
    except psycopg2.Error as e:
        print(f"❌ Ошибка запроса для {file_name}: {e}")
        conn.close()
        return None

def save_report_to_file(file_name, content, created_at):
    """Сохраняет отчет в корень папки проекта"""
    try:
        # Сохраняем прямо в корень папки
        output_filename = file_name
        
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        print(f"✅ Сохранено: {output_filename}")
        return output_filename
    
    except Exception as e:
        print(f"❌ Ошибка сохранения {file_name}: {e}")
        return None

def fetch_all_reports():
    """Извлекает все отчеты и сохраняет их локально"""
    print("=" * 60)
    print("📊 ИЗВЛЕЧЕНИЕ ОТЧЕТОВ ИЗ БД")
    print("=" * 60)
    
    results = {}
    
    for file_name in REPORT_FILES:
        print(f"\n🔍 Поиск: {file_name}...")
        report = fetch_latest_report(file_name)
        
        if report:
            file_name_db, content, created_at = report
            print(f"   Дата создания: {created_at}")
            print(f"   Размер: {len(content)} символов")
            
            # Сохраняем в файл
            saved_path = save_report_to_file(file_name_db, content, created_at)
            results[file_name_db] = {
                "path": saved_path,
                "created_at": created_at,
                "size": len(content)
            }
        else:
            print(f"   ⚠️ Файл не найден в БД")
            results[file_name] = None
    
    # Итоговый отчет
    print("\n" + "=" * 60)
    print("📋 ИТОГОВЫЙ ОТЧЕТ")
    print("=" * 60)
    
    found = sum(1 for r in results.values() if r is not None)
    total = len(results)
    
    print(f"\n✅ Найдено: {found}/{total} файлов\n")
    
    for file_name, data in results.items():
        if data:
            print(f"✓ {file_name}")
            print(f"  📁 Сохранено: {data['path']}")
            print(f"  📅 Дата: {data['created_at']}")
            print(f"  📊 Размер: {data['size']} байт\n")
        else:
            print(f"✗ {file_name} - не найден\n")
    
    return results

def display_report_preview(file_name, max_lines=10):
    """Показывает превью отчета (первые N строк)"""
    report = fetch_latest_report(file_name)
    
    if not report:
        print(f"❌ Отчет {file_name} не найден")
        return
    
    file_name_db, content, created_at = report
    
    print(f"\n{'=' * 60}")
    print(f"📄 ПРЕВЬЮ: {file_name}")
    print(f"{'=' * 60}")
    print(f"Дата создания: {created_at}\n")
    
    lines = content.split('\n')
    preview_lines = lines[:max_lines]
    
    for line in preview_lines:
        print(line)
    
    if len(lines) > max_lines:
        print(f"\n... и еще {len(lines) - max_lines} строк")

def main():
    """Главная функция"""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "preview" and len(sys.argv) > 2:
            # Показываем превью конкретного файла
            file_name = sys.argv[2]
            display_report_preview(file_name)
        
        elif command == "all":
            # Извлекаем все отчеты
            fetch_all_reports()
        
        else:
            print("Неизвестная команда")
            print("Использование:")
            print("  python fetch_reports.py all              - извлечь все отчеты")
            print("  python fetch_reports.py preview <file>   - показать превью отчета")
    else:
        # По умолчанию извлекаем все отчеты
        fetch_all_reports()

if __name__ == "__main__":
    main()