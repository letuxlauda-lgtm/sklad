import os
import psycopg2
import pandas as pd
import subprocess
import shutil
import schedule
import time
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

def export_files_from_db():
    print(f"Запуск экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Параметры подключения к БД
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    # Список файлов для экспорта
    files_to_export = [
        'otchet_dmutro.txt',
        'otchet_general.txt',
        'otchet_igor.txt',
        'otchet_inki.txt',
        'otchet_ink_general.txt',
        'otchet_ruslan.txt',
        'service_glub_analitik.txt'
    ]

    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Создаем папку для экспорта (если не существует)
        export_folder = "exports"
        os.makedirs(export_folder, exist_ok=True)
        
        # 1. Экспортируем txt файлы из таблицы automation_txt_files
        for filename in files_to_export:
            cursor.execute(
                "SELECT content, created_at FROM automation_txt_files WHERE filename = %s ORDER BY created_at DESC LIMIT 1",
                (filename,)
            )
            result = cursor.fetchone()
            
            if result:
                content, created_at = result
                # Сохраняем файл прямо в папку exports (перезаписываем если существует)
                file_path = os.path.join(export_folder, filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"Успешно экспортирован: {file_path}")
            else:
                print(f"Файл не найден в БД: {filename}")
        
        # 2. Вытягиваем таблицу inki5nedel из БД
        print("Выгрузка таблицы inki5nedel из БД...")
        cursor.execute("SELECT * FROM inki5nedel")
        rows = cursor.fetchall()
        
        # Получаем названия колонок
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'inki5nedel' ORDER BY ordinal_position")
        columns = [row[0] for row in cursor.fetchall()]
        
        # Сохраняем таблицу в CSV в папку exports
        inki5nedel_df = pd.DataFrame(rows, columns=columns)
        inki5nedel_csv_path = os.path.join(export_folder, "inki5nedel.csv")
        inki5nedel_df.to_csv(inki5nedel_csv_path, index=False, encoding='utf-8')
        print(f"Таблица inki5nedel сохранена: {inki5nedel_csv_path}")
        
        # Выводим структуру таблицы для отладки
        print("Структура таблицы inki5nedel:")
        print(inki5nedel_df.dtypes)
        print("Первые 3 строки:")
        print(inki5nedel_df.head(3))
                
    except Exception as e:
        print(f"Ошибка при работе с БД: {e}")
        with open('export_error.log', 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Ошибка БД: {e}\n")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()
    
    # 3. Запускаем скрипт podgotovka_ink_simple.py
    try:
        print("Запуск скрипта podgotovka_ink_simple.py...")
        
        # Проверяем наличие необходимых файлов
        privyazka_path = "privyazka_aparat_texnik.csv"
        if not os.path.exists(privyazka_path):
            print(f"Ошибка: файл {privyazka_path} не найден в корневой директории")
            return False
        
        # Проверяем наличие скрипта
        script_path = "podgotovka_ink_simple.py"
        if not os.path.exists(script_path):
            print(f"Ошибка: файл {script_path} не найден в корневой директории")
            print("Создайте файл podgotovka_ink_simple.py с кодом из инструкции")
            return False
        
        # Запускаем скрипт
        result = subprocess.run(
            ["python", script_path, inki5nedel_csv_path, privyazka_path],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.returncode == 0:
            print("Скрипт podgotovka_ink_simple.py успешно выполнен")
            if result.stdout:
                print(f"Вывод: {result.stdout}")
            
            # 4. Сохраняем результат как full_ink.csv в папку exports
            full_ink_path = os.path.join(export_folder, "full_ink.csv")
            
            if os.path.exists("full_ink.csv"):
                # Удаляем старый файл если существует, затем перемещаем новый
                if os.path.exists(full_ink_path):
                    os.remove(full_ink_path)
                    print(f"Удален старый файл: {full_ink_path}")
                
                # Перемещаем новый файл
                shutil.move("full_ink.csv", full_ink_path)
                print(f"Результат сохранен как: {full_ink_path}")
                
                # Проверяем результат
                try:
                    full_ink_df = pd.read_csv(full_ink_path)
                    print(f"Файл full_ink.csv загружен, размер: {full_ink_df.shape}")
                    print("Первые 3 строки результата:")
                    print(full_ink_df[['device_id', 'address', 'tech']].head(3))
                except Exception as e:
                    print(f"Ошибка при чтении результата: {e}")
            else:
                with open(full_ink_path, 'w') as f:
                    f.write("Результат не сгенерирован\n")
                print(f"Создан пустой файл: {full_ink_path}")
                
        else:
            print(f"Ошибка при выполнении скрипта podgotovka_ink_simple.py:")
            print(f"STDERR: {result.stderr}")
            if result.stdout:
                print(f"STDOUT: {result.stdout}")
            return False
            
    except Exception as e:
        print(f"Ошибка при запуске скрипта podgotovka_ink_simple.py: {e}")
        return False
    
    print(f"Экспорт завершен: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return True

def run_scheduler():
    """Запускает планировщик для ежедневного выполнения в 4:55 утра"""
    # Настраиваем расписание
    schedule.every().day.at("04:55").do(export_files_from_db)
    
    print("Планировщик запущен!")
    print("Скрипт будет выполняться каждый день в 4:55 утра")
    print("Для остановки нажмите Ctrl+C")
    print(f"Следующий запуск: {schedule.next_run()}")
    
    # Бесконечный цикл для проверки расписания
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверяем каждую минуту

if __name__ == "__main__":
    # Проверяем аргументы командной строки
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        # Запускаем немедленно
        print("Немедленный запуск экспорта...")
        success = export_files_from_db()
        if success:
            print("✅ Экспорт успешно выполнен!")
        else:
            print("❌ В процессе выполнения экспорта произошли ошибки!")
    else:
        # Запускаем планировщик
        try:
            # Сначала выполняем один раз при старте (опционально)
            # print("Первоначальный запуск экспорта...")
            # export_files_from_db()
            
            # Затем запускаем планировщик
            run_scheduler()
        except KeyboardInterrupt:
            print("\nПланировщик остановлен.")