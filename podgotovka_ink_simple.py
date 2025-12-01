import pandas as pd
import sys
import os
from datetime import datetime

def main():
    if len(sys.argv) < 3:
        print("Использование: python podgotovka_ink_simple.py <inki5nedel.csv> <privyazka_aparat_texnik.csv>")
        return 1
    
    inki5nedel_path = sys.argv[1]
    privyazka_path = sys.argv[2]
    
    try:
        print("Загрузка данных...")
        
        # Загружаем основную таблицу
        inki5nedel_df = pd.read_csv(inki5nedel_path)
        print(f"Загружена таблица inki5nedel: {inki5nedel_df.shape}")
        
        # Загружаем таблицу привязки
        privyazka_df = pd.read_csv(privyazka_path)
        print(f"Загружена таблица privyazka: {privyazka_df.shape}")
        
        # Выводим примеры данных для отладки
        print("\nПример данных из inki5nedel:")
        print(inki5nedel_df.head(3))
        print("\nПример данных из privyazka:")
        print(privyazka_df.head(3))
        
        # Определяем правильные названия колонок
        # В inki5nedel: device_id для ID, tech для имени техника
        # В privyazka: id_terem для ID, texnik для имени техника
        
        # Преобразуем ID к одинаковому типу для сравнения
        inki5nedel_df['device_id'] = inki5nedel_df['device_id'].astype(str).str.strip()
        privyazka_df['id_terem'] = privyazka_df['id_terem'].astype(str).str.strip()
        
        # Создаем словарь для быстрого поиска техника по ID
        technik_dict = dict(zip(privyazka_df['id_terem'], privyazka_df['texnik']))
        
        print(f"\nСловарь привязок: {technik_dict}")
        
        # Заменяем столбец tech в inki5nedel на данные из словаря
        # Если ID не найден, оставляем текущее значение
        inki5nedel_df['tech'] = inki5nedel_df['device_id'].map(technik_dict).fillna(inki5nedel_df['tech'])
        
        print("\nРезультат объединения:")
        print(inki5nedel_df[['device_id', 'address', 'tech']].head(10))
        
        # Сохраняем результат (перезаписываем если файл существует)
        output_path = 'full_ink.csv'
        inki5nedel_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"\nРезультат сохранен в: {output_path}")
        print(f"Размер результата: {inki5nedel_df.shape}")
        
        # Проверяем несколько примеров
        print("\nПроверка замены:")
        test_ids = ['153', '240', '297']
        for test_id in test_ids:
            test_rows = inki5nedel_df[inki5nedel_df['device_id'].astype(str).str.strip() == test_id]
            if not test_rows.empty:
                print(f"ID {test_id}: техник = {test_rows.iloc[0]['tech']}")
        
        return 0
        
    except Exception as e:
        print(f"Ошибка при обработке данных: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)