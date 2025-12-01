import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# --- КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ ---
# Формат: postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DB_NAME
DB_URI = "postgresql+psycopg2://dataanalyst:))vodoliJuser2025@49.13.142.186:5432/datavodolij"

def generate_ink_report():
    try:
        logging.info("⏳ Подключение к БД и загрузка inki5nedel...")
        
        # 1. Создаем движок подключения
        engine = create_engine(DB_URI)
        
        # 2. Загружаем таблицу в Pandas DataFrame
        query = "SELECT * FROM inki5nedel"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            logging.warning("⚠️ Таблица inki5nedel пуста.")
            with open("otchet_ink_general.txt", "w", encoding="utf-8") as f:
                f.write("Нет данных в таблице inki5nedel.")
            return

        # 3. Обработка дат
        # Приводим колонку 'date' к формату datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Текущее время и граничная дата (6 дней назад)
        now = datetime.now()
        cutoff_date = now - timedelta(days=6)
        
        logging.info(f"📅 Дата отсечения (Сдано/На руках): {cutoff_date.strftime('%Y-%m-%d')}")

        # 4. Разделение данных
        # "СДАНО" - дата меньше или равна граничной (старые)
        df_sdano = df[df['date'] <= cutoff_date].copy()
        
        # "НА РУКАХ" - дата больше граничной (свежие)
        df_na_rukah = df[df['date'] > cutoff_date].copy()

        # 5. Функция для группировки и подсчета сумм
        def calculate_stats(dataframe):
            if dataframe.empty:
                return pd.DataFrame()
            
            # Группируем по технику (descr), суммируем купюры и монеты
            # reset_index превращает индексы обратно в колонки для удобства
            stats = dataframe.groupby('descr')[['banknotes', 'coins']].sum().reset_index()
            
            # Добавляем общую сумму
            stats['total'] = stats['banknotes'] + stats['coins']
            
            # Сортируем по убыванию общей суммы
            stats = stats.sort_values(by='total', ascending=False)
            return stats

        stats_sdano = calculate_stats(df_sdano)
        stats_na_rukah = calculate_stats(df_na_rukah)

        # 6. Запись в файл otchet_ink_general.txt
        filename = "otchet_ink_general.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"📊 ОТЧЕТ ПО ИНКАССАЦИЯМ (Сформирован: {now.strftime('%Y-%m-%d %H:%M')})\n")
            f.write(f"📅 Правило разделения: 6 дней (до {cutoff_date.strftime('%d.%m.%Y')})\n")
            f.write("="*40 + "\n\n")

            # БЛОК: НА РУКАХ
            f.write("✋ НА РУКАХ (Свежие, <= 6 дней):\n")
            if not stats_na_rukah.empty:
                f.write(f"{'Техник':<15} | {'Купюры':<10} | {'Монеты':<10} | {'ВСЕГО':<10}\n")
                f.write("-" * 55 + "\n")
                total_na_rukah = 0
                for _, row in stats_na_rukah.iterrows():
                    f.write(f"{row['descr']:<15} | {row['banknotes']:<10} | {row['coins']:<10} | {row['total']:<10}\n")
                    total_na_rukah += row['total']
                f.write("-" * 55 + "\n")
                f.write(f"ИТОГО НА РУКАХ: {total_na_rukah}\n\n")
            else:
                f.write("Нет данных.\n\n")

            f.write("="*40 + "\n\n")

            # БЛОК: СДАНО
            f.write("✅ СДАНО (Архив, > 6 дней):\n")
            if not stats_sdano.empty:
                f.write(f"{'Техник':<15} | {'Купюры':<10} | {'Монеты':<10} | {'ВСЕГО':<10}\n")
                f.write("-" * 55 + "\n")
                total_sdano = 0
                for _, row in stats_sdano.iterrows():
                    f.write(f"{row['descr']:<15} | {row['banknotes']:<10} | {row['coins']:<10} | {row['total']:<10}\n")
                    total_sdano += row['total']
                f.write("-" * 55 + "\n")
                f.write(f"ИТОГО СДАНО: {total_sdano}\n")
            else:
                f.write("Нет данных.\n")

        logging.info(f"✅ Файл {filename} успешно создан!")

    except Exception as e:
        logging.error(f"❌ Ошибка при создании отчета: {e}")

# Если запускаем этот файл напрямую, создаем отчет
if __name__ == "__main__":
    generate_ink_report()