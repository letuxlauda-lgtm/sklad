import os
import html
import psycopg2
import subprocess
import asyncio
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove, FSInputFile
)

# Убедитесь, что эти файлы существуют в папке с ботом
import database as db
import create_ink_report

router = Router()

# =======================================================
#  ЗАГРУЖАЕМ .env и параметры PostgreSQL
# =======================================================

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_pg_conn():
    """Подключение к PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# =======================================================
#  МЕНЮ SUPER ADMIN
# =======================================================

def get_super_menu():
    kb = [
        [KeyboardButton(text="📃денний звіт"), KeyboardButton(text="🗃️завдання,картки")],
        [KeyboardButton(text="🛒стіл замовлень"), KeyboardButton(text="файл карты")],
        [KeyboardButton(text="💰інкі 5тиж"), KeyboardButton(text="💰інкі 1тиж")],
        [KeyboardButton(text="📈звіт service"), KeyboardButton(text="📉звіт")],
        [KeyboardButton(text="📊service big звіт")],
        [KeyboardButton(text="👇вихід з ролі")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


class SuperRole(StatesGroup):
    online = State()

# =======================================================
# Новая общая функция — запускает podgotovka_ink_simple.py и ждёт его выполнения
# =======================================================
async def run_podgotovka_ink(message: types.Message) -> bool:
    status_msg = await message.answer("⏳ Оновлюю дані інкасацій (podgotovka_ink_simple.py)...")
    
    try:
        process = await asyncio.create_subprocess_exec(
            "python3", "podgotovka_ink_simple.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        await status_msg.delete()
        
        if process.returncode != 0:
            error_msg = stderr.decode("utf-8") or stdout.decode("utf-8")
            await message.answer(f"⚠️ Помилка в podgotovka_ink_simple.py:\n```{error_msg}```", parse_mode="Markdown")
            return False
        return True
    except Exception as e:
        await status_msg.delete()
        await message.answer(f"❌ Виняток при запуску podgotovka_ink_simple.py: {e}")
        return False

# =======================================================
#  ФУНКЦИЯ ОБНОВЛЕНИЯ ОТЧЕТОВ ИЗ БД
# =======================================================

async def refresh_reports_from_db(message: types.Message):
    """
    Запускает fetch_reports.py для обновления отчетов из БД.
    """
    status_msg = await message.answer("⏳ оновлюю дані...")
    
    try:
        # Запускаем fetch_reports.py
        process = await asyncio.create_subprocess_exec(
            "python3", "fetch_reports.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        await status_msg.delete()
        
        if process.returncode == 0:
            return True
        else:
            await message.answer(f"⚠️ Помилка оновлення:\n{stderr.decode()}")
            return False
            
    except Exception as e:
        await status_msg.delete()
        await message.answer(f"❌ Помилка скрипта: {e}")
        return False


# =======================================================
# 🎯 ВХОД В РОЛЬ SUPER ADMIN
# =======================================================

@router.message(F.text.lower() == "sup1")
async def super_login(message: types.Message, state: FSMContext):
    await state.set_state(SuperRole.online)
    await message.answer("😎 Вітаю, Бос! Ось ваше меню:", reply_markup=get_super_menu())


# =======================================================
# 📄 ДНЕВНОЙ ОТЧЕТ (Кнопка: "📃денний звіт")
# =======================================================

@router.message(SuperRole.online, F.text.contains("денний звіт"))
async def send_report(message: types.Message):
    # ОБНОВЛЯЕМ ОТЧЕТЫ ИЗ БД
    await refresh_reports_from_db(message)
    
    # Получаем отчет напрямую из БД
    content = db.get_latest_general_report()
    
    if content and content.strip():
        safe_content = html.escape(content)
        
        # Если отчет очень длинный, разбиваем на части
        if len(safe_content) > 4000:
            for i in range(0, len(safe_content), 4000):
                await message.answer(f"<pre>{safe_content[i:i+4000]}</pre>", parse_mode="HTML")
        else:
            await message.answer(f"<pre>{safe_content}</pre>", parse_mode="HTML")
    else:
        await message.answer("⚠️ Денний звіт не знайдено у базі даних.")


# =======================================================
# 📊 АНАЛИТИКА (Кнопка: "🗃️завдання,картки")
# =======================================================

@router.message(SuperRole.online, F.text.contains("завдання,картки"))
async def show_analytics(message: types.Message):
    await message.answer("⏳ Збираю аналітику за 30 днів...")

    try:
        top15 = db.get_super_analytics_top15()
        report = "🏆 <b>ТОП-15 Довгих задач:</b>\n"

        if top15:
            for row in top15:
                # Распаковка зависит от вашей функции get_super_analytics_top15
                # Предполагаем: source, t_id, terem, adr, tex, time_min
                source, t_id, terem, adr, tex, time_min = row
                time_str = f"{time_min:.1f} мин" if time_min else "N/A"
                report += f"🔻 {source} (ID:{t_id}) | {adr} | {tex} | ⏳ <b>{time_str}</b>\n"
        else:
            report += "Немає даних.\n"

        await message.answer(report, parse_mode="HTML")

        # Среднее время
        avg_report = "⏱ <b>Середній час виконання (хв):</b>\n"
        try:
            avg_ruslan = db.get_avg_time_by_texnik('ruslan') or 0
            avg_igor = db.get_avg_time_by_texnik('igor') or 0
            avg_dmutro = db.get_avg_time_by_texnik('dmutro') or 0
            
            avg_report += f"👤 Ruslan: <b>{avg_ruslan:.1f}</b>\n"
            avg_report += f"👤 Igor: <b>{avg_igor:.1f}</b>\n"
            avg_report += f"👤 Dmutro: <b>{avg_dmutro:.1f}</b>"
        except Exception:
            avg_report += "Помилка розрахунку середнього часу."

        await message.answer(avg_report, parse_mode="HTML")

        # Повторные поломки
        recurring = db.get_recurring_issues()
        rec_report = "🔄 <b>Повторні поломки (>1 раза):</b>\n"

        if recurring:
            for row in recurring:
                terem_id, count, adr, tex = row
                rec_report += f"⚠️ <b>{count} раз(а)</b>: {adr} (ID:{terem_id}) [{tex}]\n"
        else:
            rec_report += "Повторів немає."

        await message.answer(rec_report, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Помилка аналітики: {e}")



# =======================================================
# 💰 ИНКАССАЦИИ ЗА НЕДЕЛЮ (Кнопка: "💰інкі 1тиж")
# =======================================================

@router.message(SuperRole.online, F.text.contains("інкі 1тиж"))
async def report_inki_week(message: types.Message):
    """Формирует отчет по инкассациям за 7 дней из файла exports/inki5nedel.csv"""
    
    status_msg = await message.answer("⏳ Формирую отчет по инкассациям за 7 дней...")

    try:
        # Путь к файлу
        csv_path = os.path.join("exports", "inki5nedel.csv")
        
        if not os.path.exists(csv_path):
            await status_msg.edit_text("❌ Файл exports/inki5nedel.csv не найден!")
            return

        # Читаем CSV файл
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        
        # Проверяем наличие необходимых колонок
        required_cols = ['device_id', 'address', 'date', 'banknotes', 'coins', 'tech']
        if not all(col in df.columns for col in required_cols):
            await status_msg.edit_text("❌ CSV файл не содержит необходимых колонок!")
            return

        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # Конвертируем суммы в числа
        df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
        df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
        
        # Заполняем пустые техники
        df['tech'] = df['tech'].fillna('unknown')
        
        # Фильтруем данные за последние 7 дней
        cutoff_date = datetime.now() - timedelta(days=7)
        df_week = df[df['date'] >= cutoff_date].copy()
        
        if df_week.empty:
            await status_msg.edit_text("📂 За последние 7 дней инкассаций не найдено.")
            return

        # Убедимся, что у всех записей есть техник
        missing_tech = df_week[df_week['tech'].isin(['unknown', '   -   ', ''])]
        if not missing_tech.empty:
            # Попробуем заполнить недостающие техники из базы данных
            from database import ADDRESS_DB
            
            # Создаем словарь для поиска техника по device_id
            tech_mapping = {}
            for item in ADDRESS_DB:
                tech_mapping[item['id_terem']] = item['texnik']
            
            # Функция для поиска техника
            def find_tech_by_device_id(device_id):
                return tech_mapping.get(device_id, 'unknown')
            
            # Применяем поиск техника для записей без техника
            for idx, row in missing_tech.iterrows():
                device_id = row['device_id']
                if pd.notna(device_id):
                    try:
                        tech = find_tech_by_device_id(int(device_id))
                        df_week.at[idx, 'tech'] = tech
                    except (ValueError, TypeError):
                        pass

        # Формируем отчет
        report_lines = []
        report_lines.append("==================================================")
        report_lines.append("📊 ОТЧЕТ ПО ИНКАСАЦИЯМ за 7 дней")
        report_lines.append("==================================================")
        
        # Группируем по техникам (только известные техники)
        known_techs = ['ruslan', 'igor', 'dmutro']
        df_known = df_week[df_week['tech'].isin(known_techs)]
        df_unknown = df_week[~df_week['tech'].isin(known_techs)]
        
        # Для подозрительных инкассаций
        suspicious = []
        
        # Обрабатываем известных техников
        for tech in known_techs:
            tech_data = df_known[df_known['tech'] == tech].copy()
            
            if tech_data.empty:
                continue
                
            report_lines.append(f"{tech.upper()}")
            
            # Группируем по device_id для этого техника
            device_totals = tech_data.groupby('device_id').agg({
                'banknotes': 'sum',
                'coins': 'sum',
                'address': 'first',
                'date': 'max'
            }).reset_index()
            
            # Отделяем подозрительные инкассации
            normal_devices = []
            suspicious_devices = []
            
            for _, row in device_totals.iterrows():
                device_id = row['device_id']
                address = row['address'] if pd.notna(row['address']) else "—"
                banknotes = row['banknotes']
                coins = row['coins']
                date_str = row['date'].strftime('%d.%m.%Y') if pd.notna(row['date']) else "—"
                
                # Проверяем на подозрительные суммы
                if banknotes > 18000 or coins > 18000:
                    suspicious_devices.append({
                        'device_id': device_id,
                        'address': address,
                        'banknotes': banknotes,
                        'coins': coins,
                        'date': date_str,
                        'tech': tech
                    })
                else:
                    normal_devices.append(row)
            
            # Выводим только нормальные аппараты
            for row in normal_devices:
                device_id = row['device_id']
                banknotes = row['banknotes']
                coins = row['coins']
                
                report_lines.append(f"{device_id}, банкноты: {banknotes:,.0f} грн, монеты: {coins:,.0f} грн".replace(',', ' '))
            
            # Суммы по технику (только нормальные устройства)
            if normal_devices:
                normal_df = pd.DataFrame(normal_devices)
                tech_banknotes = normal_df['banknotes'].sum()
                tech_coins = normal_df['coins'].sum()
                tech_total = tech_banknotes + tech_coins
                
                report_lines.append("(далее считаем сумму банкнот, и сумму монет)")
                report_lines.append(f"сумма банкнот: {tech_banknotes:,.0f} грн, сумма монет: {tech_coins:,.0f} грн, общая: {tech_total:,.0f} грн".replace(',', ' '))
            else:
                report_lines.append("(нет нормальных инкассаций)")
                report_lines.append("сумма банкнот: 0 грн, сумма монет: 0 грн, общая: 0 грн")
            
            report_lines.append("==================================================")
            
            # Добавляем подозрительные в общий список
            suspicious.extend(suspicious_devices)
        
        # Обрабатываем неизвестных техников (если есть)
        if not df_unknown.empty:
            report_lines.append("НЕИЗВЕСТНЫЕ ТЕХНИКИ")
            
            unknown_totals = df_unknown.groupby('device_id').agg({
                'banknotes': 'sum',
                'coins': 'sum',
                'address': 'first',
                'date': 'max'
            }).reset_index()
            
            # Отделяем подозрительные инкассации
            normal_unknown = []
            suspicious_unknown = []
            
            for _, row in unknown_totals.iterrows():
                device_id = row['device_id']
                address = row['address'] if pd.notna(row['address']) else "—"
                banknotes = row['banknotes']
                coins = row['coins']
                date_str = row['date'].strftime('%d.%m.%Y') if pd.notna(row['date']) else "—"
                
                # Проверяем на подозрительные суммы
                if banknotes > 18000 or coins > 18000:
                    suspicious_unknown.append({
                        'device_id': device_id,
                        'address': address,
                        'banknotes': banknotes,
                        'coins': coins,
                        'date': date_str,
                        'tech': 'unknown'
                    })
                else:
                    normal_unknown.append(row)
            
            # Выводим только нормальные аппараты
            for row in normal_unknown:
                device_id = row['device_id']
                banknotes = row['banknotes']
                coins = row['coins']
                
                report_lines.append(f"{device_id}, банкноты: {banknotes:,.0f} грн, монеты: {coins:,.0f} грн".replace(',', ' '))
            
            # Суммы по неизвестным техникам (только нормальные устройства)
            if normal_unknown:
                normal_unknown_df = pd.DataFrame(normal_unknown)
                unknown_banknotes = normal_unknown_df['banknotes'].sum()
                unknown_coins = normal_unknown_df['coins'].sum()
                unknown_total = unknown_banknotes + unknown_coins
                
                report_lines.append("(далее считаем сумму банкнот, и сумму монет)")
                report_lines.append(f"сумма банкнот: {unknown_banknotes:,.0f} грн, сумма монет: {unknown_coins:,.0f} грн, общая: {unknown_total:,.0f} грн".replace(',', ' '))
            else:
                report_lines.append("(нет нормальных инкассаций)")
                report_lines.append("сумма банкнот: 0 грн, сумма монет: 0 грн, общая: 0 грн")
            
            report_lines.append("==================================================")
            
            # Добавляем подозрительные в общий список
            suspicious.extend(suspicious_unknown)
        
        # Добавляем подозрительные инкассации
        if suspicious:
            report_lines.append("⁉️ПОДОЗРИТЕЛЬНЫЕ ИНКАССАЦИИ⁉️")
            report_lines.append("==================================================")
            report_lines.append("(аппараты на которых сумма банкнот или сумма копеек более 18 тыс грн)")
            report_lines.append("")
            
            for item in suspicious:
                report_lines.append(f"ID: {item['device_id']}, Адрес: {item['address']}")
                report_lines.append(f"Банкноты: {item['banknotes']:,.0f} грн, Монеты: {item['coins']:,.0f} грн".replace(',', ' '))
                report_lines.append(f"Дата: {item['date']}, Техник: {item['tech']}")
                report_lines.append("---")
        else:
            report_lines.append("⁉️ПОДОЗРИТЕЛЬНЫЕ ИНКАССАЦИИ⁉️")
            report_lines.append("==================================================")
            report_lines.append("Подозрительных инкассаций (свыше 18,000 грн) не обнаружено")
        
        # Формируем финальный текст
        full_report = "\n".join(report_lines)
        
        await status_msg.delete()
        
        # Отправляем отчет частями если он слишком длинный
        if len(full_report) <= 4000:
            await message.answer(f"<pre>{full_report}</pre>", parse_mode="HTML")
        else:
            chunks = [full_report[i:i+3800] for i in range(0, len(full_report), 3800)]
            for chunk in chunks:
                await message.answer(f"<pre>{chunk}</pre>", parse_mode="HTML")
                await asyncio.sleep(0.3)

    except Exception as e:
        await status_msg.delete()
        await message.answer(f"❌ Ошибка при формировании отчета: {e}")
        import logging
        import traceback
        logging.error(f"Ошибка недельного отчета инкассаций: {e}")
        logging.error(traceback.format_exc())
        
# =======================================================
# 💰 ИНКАССАЦИИ ЗА 5 НЕДЕЛЬ (Кнопка: "💰інкі 5тиж")
# =======================================================
@router.message(SuperRole.online, F.text.contains("інкі 5тиж"))
async def report_inki_5weeks(message: types.Message):
    """Формирует отчет по инкассациям за 5 недель из файла exports/inki5nedel.csv"""
    
    status_msg = await message.answer("⏳ Формирую отчет по инкассациям за 5 недель...")

    try:
        # Путь к файлу
        csv_path = os.path.join("exports", "inki5nedel.csv")
        
        if not os.path.exists(csv_path):
            await status_msg.edit_text("❌ Файл exports/inki5nedel.csv не найден!")
            return

        # Читаем CSV файл
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        
        # Проверяем наличие необходимых колонок
        required_cols = ['device_id', 'address', 'date', 'banknotes', 'coins', 'tech']
        if not all(col in df.columns for col in required_cols):
            await status_msg.edit_text("❌ CSV файл не содержит необходимых колонок!")
            return

        # Конвертируем дату
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # Конвертируем суммы в числа
        df['banknotes'] = pd.to_numeric(df['banknotes'], errors='coerce').fillna(0)
        df['coins'] = pd.to_numeric(df['coins'], errors='coerce').fillna(0)
        
        # Заполняем пустые техники
        df['tech'] = df['tech'].fillna('unknown')
        
        # Убедимся, что у всех записей есть техник
        missing_tech = df[df['tech'].isin(['unknown', '   -   ', ''])]
        if not missing_tech.empty:
            # Попробуем заполнить недостающие техники из базы данных
            from database import ADDRESS_DB
            
            # Создаем словарь для поиска техника по device_id
            tech_mapping = {}
            for item in ADDRESS_DB:
                tech_mapping[item['id_terem']] = item['texnik']
            
            # Функция для поиска техника
            def find_tech_by_device_id(device_id):
                return tech_mapping.get(device_id, 'unknown')
            
            # Применяем поиск техника для записей без техника
            for idx, row in missing_tech.iterrows():
                device_id = row['device_id']
                if pd.notna(device_id):
                    try:
                        tech = find_tech_by_device_id(int(device_id))
                        df.at[idx, 'tech'] = tech
                    except (ValueError, TypeError):
                        pass

        # Определяем даты
        today = datetime.now().date()
        cutoff_date = pd.Timestamp(today) - pd.Timedelta(days=7)
        
        # Разделяем на нормальные и подозрительные инкассации
        normal_data = []
        suspicious_data = []
        
        # Группируем по device_id и tech для выявления подозрительных
        grouped = df.groupby(['device_id', 'tech']).agg({
            'banknotes': 'sum',
            'coins': 'sum',
            'address': 'first',
            'date': ['min', 'max']
        }).reset_index()
        
        # Упрощаем колонки после группировки
        grouped.columns = ['device_id', 'tech', 'banknotes', 'coins', 'address', 'date_min', 'date_max']
        
        for _, row in grouped.iterrows():
            device_id = row['device_id']
            tech = row['tech']
            banknotes = row['banknotes']
            coins = row['coins']
            address = row['address'] if pd.notna(row['address']) else "—"
            date_min = row['date_min']
            date_max = row['date_max']
            
            # Пропускаем записи с неизвестными техниками
            if tech in ['unknown', '   -   ', '']:
                continue
                
            # Проверяем на подозрительные суммы (теперь порог 20,000 грн)
            if banknotes > 20000 or coins > 20000:
                suspicious_data.append({
                    'device_id': device_id,
                    'tech': tech,
                    'banknotes': banknotes,
                    'coins': coins,
                    'address': address,
                    'date_min': date_min,
                    'date_max': date_max
                })
            else:
                normal_data.append(row)
        
        # Формируем отчет
        report_lines = []
        report_lines.append("📊 ОТЧЕТ ПО ИНКАСАЦИЯМ")
        report_lines.append("=" * 50)
        report_lines.append(f"Отчет сформирован: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        report_lines.append("")
        
        # Группируем нормальные данные по техникам
        known_techs = ['ruslan', 'igor', 'dmutro']
        
        # Статистика для сводки
        total_banknotes = 0
        total_coins = 0
        total_inkasations = 0
        
        # Обрабатываем известных техников
        for tech in known_techs:
            tech_normal_data = [row for row in normal_data if row['tech'] == tech]
            
            if not tech_normal_data:
                continue
                
            # Создаем DataFrame для этого техника
            tech_df = pd.DataFrame(tech_normal_data)
            
            report_lines.append(f"🧑‍💼 ТЕХНИК: {tech.upper()}")
            report_lines.append("-" * 40)
            
            # Разделяем на сдано и на руках по дате
            sdano_data = tech_df[tech_df['date_max'] < cutoff_date]
            na_rukah_data = tech_df[tech_df['date_max'] >= cutoff_date]
            
            # СДАНО
            if not sdano_data.empty:
                sdano_bank = sdano_data['banknotes'].sum()
                sdano_coins = sdano_data['coins'].sum()
                sdano_total = sdano_bank + sdano_coins
                
                period_start = sdano_data['date_min'].min().strftime('%d.%m.%Y')
                period_end = sdano_data['date_max'].max().strftime('%d.%m.%Y')
                
                report_lines.append("✅ СДАНО:")
                report_lines.append(f"   Период: {period_start} - {period_end}")
                report_lines.append(f"   Банкноты: {sdano_bank:,.0f} грн".replace(',', ' '))
                report_lines.append(f"   Монеты: {sdano_coins:,.0f} грн".replace(',', ' '))
                report_lines.append(f"   ОБЩАЯ: {sdano_total:,.0f} грн (должна быть сдана)".replace(',', ' '))
                report_lines.append("")
            
            # НА РУКАХ
            if not na_rukah_data.empty:
                na_rukah_bank = na_rukah_data['banknotes'].sum()
                na_rukah_coins = na_rukah_data['coins'].sum()
                na_rukah_total = na_rukah_bank + na_rukah_coins
                
                report_lines.append("💰 НА РУКАХ:")
                report_lines.append(f"   Банкноты: {na_rukah_bank:,.0f} грн".replace(',', ' '))
                report_lines.append(f"   Монеты: {na_rukah_coins:,.0f} грн".replace(',', ' '))
                report_lines.append(f"   ОБЩАЯ: {na_rukah_total:,.0f} грн".replace(',', ' '))
                
                # Разбивка по дням (для последней недели)
                last_week_data = df[
                    (df['tech'] == tech) & 
                    (df['date'] >= cutoff_date) &
                    (~((df['banknotes'] > 20000) | (df['coins'] > 20000)))  # Исключаем подозрительные (порог 20,000)
                ]
                
                if not last_week_data.empty:
                    daily = last_week_data.groupby(last_week_data['date'].dt.date).agg({
                        'banknotes': 'sum',
                        'coins': 'sum'
                    })
                    
                    for day, row in daily.iterrows():
                        day_total = row['banknotes'] + row['coins']
                        report_lines.append(f"   📅 {day.strftime('%d.%m.%Y')}: {day_total:,.0f} грн".replace(',', ' '))
                
                report_lines.append("")
            
            # Добавляем к общей статистике
            total_banknotes += tech_df['banknotes'].sum()
            total_coins += tech_df['coins'].sum()
            total_inkasations += len(tech_df)
        
        # СВОДНАЯ СТАТИСТИКА (только нормальные инкассации)
        report_lines.append("📈 СВОДНАЯ СТАТИСТИКА")
        report_lines.append("-" * 40)
        report_lines.append(f"Общая сумма банкнот: {total_banknotes:,.0f} грн".replace(',', ' '))
        report_lines.append(f"Общая сумма монет: {total_coins:,.0f} грн".replace(',', ' '))
        report_lines.append(f"ОБЩИЙ ИТОГ: {total_banknotes + total_coins:,.0f} грн".replace(',', ' '))
        report_lines.append(f"Всего инкасаций: {total_inkasations}")
        
        if normal_data:
            normal_df = pd.DataFrame(normal_data)
            period_start = normal_df['date_min'].min().strftime('%d.%m.%Y')
            period_end = normal_df['date_max'].max().strftime('%d.%m.%Y')
            report_lines.append(f"Период данных: {period_start} - {period_end}")
        
        # Добавляем подозрительные инкассации
        if suspicious_data:
            report_lines.append("")
            report_lines.append("==================================================")
            report_lines.append("⁉️ПОДОЗРИТЕЛЬНЫЕ ИНКАССАЦИИ⁉️")
            report_lines.append("==================================================")
            report_lines.append("(аппараты на которых сумма банкнот или монет более 20 тыс грн)")
            report_lines.append("")
            
            for item in suspicious_data:
                report_lines.append(f"ID: {item['device_id']}, Адрес: {item['address']}")
                report_lines.append(f"Банкноты: {item['banknotes']:,.0f} грн, Монеты: {item['coins']:,.0f} грн".replace(',', ' '))
                report_lines.append(f"Период: {item['date_min'].strftime('%d.%m.%Y')} - {item['date_max'].strftime('%d.%m.%Y')}")
                report_lines.append(f"Техник: {item['tech']}")
                report_lines.append("---")
        
        # Формируем финальный текст
        full_report = "\n".join(report_lines)
        
        await status_msg.delete()
        
        # Отправляем отчет частями если он слишком длинный
        if len(full_report) <= 4000:
            await message.answer(f"<pre>{full_report}</pre>", parse_mode="HTML")
        else:
            chunks = [full_report[i:i+3800] for i in range(0, len(full_report), 3800)]
            for chunk in chunks:
                await message.answer(f"<pre>{chunk}</pre>", parse_mode="HTML")
                await asyncio.sleep(0.3)

    except Exception as e:
        await status_msg.delete()
        await message.answer(f"❌ Ошибка при формировании отчета: {e}")
        import logging
        import traceback
        logging.error(f"Ошибка 5-недельного отчета инкассаций: {e}")
        logging.error(traceback.format_exc())

# =======================================================
# 🛒 СТОЛ ЗАКАЗОВ (Кнопка: "🛒стіл замовлень")
# =======================================================

@router.message(SuperRole.online, F.text.contains("стіл замовлень"))
async def show_admin_stol(message: types.Message):
    rows = db.get_all_stol_zakazov("open")
    if not rows:
        await message.answer("Стол заказов пуст.")
        return

    await message.answer(f"📦 Активных заказов: {len(rows)}")

    for row in rows:
        z_id, texnik, zakaz, date = row
        text = f"🆔 {z_id} | 👤 {texnik}\n🛒 {zakaz}\n📅 {date}"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Обработано", callback_data=f"stol_close:{z_id}")]
        ])
        await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("stol_close:"))
async def process_stol_close(callback: types.CallbackQuery):
    try:
        _, z_id = callback.data.split(":")
        if db.close_stol_zakaz(z_id):
            await callback.message.edit_text(
                f"{callback.message.text}\n\n✅ <b>ОБРОБЛЕНО</b>",
                parse_mode="HTML",
                reply_markup=None
            )
            await callback.answer("Заказ закрыт!")
        else:
            await callback.answer("Ошибка БД", show_alert=True)
    except Exception as e:
        await callback.answer(f"Ошибка: {e}")


# =======================================================
# 📄 ОТЧЕТЫ (Кнопки: "📉звіт", "📈звіт service")
# =======================================================

@router.message(SuperRole.online, F.text.contains("📉звіт"))
async def send_inki_from_db(message: types.Message):
    """
    Отправляет общий отчет по инкассациям (otchet_inki.txt из базы)
    """
    await refresh_reports_from_db(message)
    
    content = db.get_latest_ink_report()
    if not content:
        await message.answer("⚠️ В базе нет отчета по инкассациям.")
        return

    safe = html.escape(content)
    if len(safe) > 4000:
        for i in range(0, len(safe), 4000):
            await message.answer(f"<pre>{safe[i:i+4000]}</pre>", parse_mode="HTML")
    else:
        await message.answer(f"<pre>{safe}</pre>", parse_mode="HTML")


@router.message(SuperRole.online, F.text.contains("звіт service"))
async def send_service_all(message: types.Message):
    """
    Отправляет отчет по сервису
    """
    await refresh_reports_from_db(message)
    content = db.get_latest_service_report()
    
    if content and content.strip():
        safe_content = html.escape(content)
        if len(safe_content) > 4000:
            for i in range(0, len(safe_content), 4000):
                await message.answer(f"<pre>{safe_content[i:i+4000]}</pre>", parse_mode="HTML")
        else:
            await message.answer(f"<pre>{safe_content}</pre>", parse_mode="HTML")
    else:
        await message.answer("⚠️ Service отчет не найден.")


# =======================================================
# 🗺 ФАЙЛ КАРТЫ (Кнопка: "файл карты")
# =======================================================

@router.message(SuperRole.online, F.text.contains("файл карты"))
async def send_map_file(message: types.Message):
    await send_file_safe(message, "interactive_routes_map.html", "Интерактивная карта")


# =======================================================
# 📊 SERVICE BIG ZVIT (Кнопка: "📊service big звіт")
# =======================================================

@router.message(SuperRole.online, F.text.contains("service big звіт"))
async def send_deep_analysis(message: types.Message):
    await refresh_reports_from_db(message)
    
    file_path = "analysis_report.txt"
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        await message.answer(content if content.strip() else "📂 Файл пуст.")
    else:
        await message.answer("⚠️ Файл analysis_report.txt не найден.")


# =======================================================
# 🚪 ВЫХОД ИЗ РОЛИ (Кнопка: "👇вихід з ролі")
# =======================================================

@router.message(F.text.contains("вихід з ролі"))
async def exit_super(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Ви вийшли з режиму Адміністратора.", reply_markup=ReplyKeyboardRemove())


# =======================================================
# ⚙️ УТИЛИТА ОТПРАВКИ ФАЙЛОВ
# =======================================================

async def send_file_safe(message, filename, caption):
    if os.path.exists(filename):
        try:
            file = FSInputFile(filename)
            await message.answer_document(file, caption=caption)
        except Exception as e:
            await message.answer(f"Ошибка при отправке {filename}: {e}")
    else:
        await message.answer(f"⚠️ Файл {filename} не найден.")