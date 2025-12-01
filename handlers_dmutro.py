import os
import sys
import asyncio
from datetime import datetime
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove, FSInputFile
import database as db

router = Router()

def get_dmutro_menu():
    kb = [
        [KeyboardButton(text="⚙️звіт по роботі"), KeyboardButton(text="💳завдання,замовленя")],
        [KeyboardButton(text="🛒стіл замовлень"), KeyboardButton(text="⬇️витрати")],
        [KeyboardButton(text="вийти з ролі")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- СОСТОЯНИЕ ДЛЯ ДМИТРА ---
class DmutroRole(StatesGroup):
    online = State()

class StolZakazovState(StatesGroup):
    waiting_for_item = State()

class ZatratuState(StatesGroup):
    waiting_for_name = State()
    waiting_for_sum = State()

# 🚪 ВХОД (dmut2)
@router.message(F.text.lower() == "dmut2")
async def dmutro_login(message: types.Message, state: FSMContext):
    try:
        db.init_dmutro_tables() 
        await state.set_state(DmutroRole.online)
        await message.answer("👨‍🔧 Привіт, Дмитро! Твоє меню:", reply_markup=get_dmutro_menu())
    except Exception as e:
        await message.answer(f"❌ Помилка БД при вході: {e}")


# =======================================================
# 📄 ОТЧЕТ (ИСПРАВЛЕН ЗАПУСК)
# =======================================================
@router.message(DmutroRole.online, F.text == "⚙️звіт по роботі") 
async def send_report(message: types.Message):
    status_msg = await message.answer("⏳ Оновлюю дані, зачекайте...")

    try:
        # --- 1. ЗАПУСК СКРИПТА ---
        print("DEBUG: Запускаю fetch_reports.py...") # Лог в консоль
        process = await asyncio.create_subprocess_exec(
            sys.executable, "fetch_reports.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_text = stderr.decode().strip()
            print(f"DEBUG: Ошибка скрипта: {error_text}")
            await status_msg.delete()
            await message.answer(f"❌ Помилка скрипта fetch_reports:\n{error_text}")
            return

        print("DEBUG: Скрипт выполнен успешно.")

        # --- 2. ПОЛУЧЕНИЕ ВРЕМЕНИ ФАЙЛА ---
        # Используем абсолютный путь, чтобы точно найти файл рядом со скриптом бота
        current_dir = os.getcwd() # Текущая папка, где лежит бот
        filename = 'otchet_dmutro.txt'
        file_path = os.path.join(current_dir, filename)
        
        file_time_str = "Невідомо"
        
        if os.path.exists(file_path):
            # Получаем время модификации
            mod_time = os.path.getmtime(file_path)
            # Конвертируем дату
            dt_obj = datetime.fromtimestamp(mod_time)
            file_time_str = dt_obj.strftime("%d.%m.%Y %H:%M:%S")
            print(f"DEBUG: Файл найден. Время: {file_time_str}")
        else:
            print(f"DEBUG: Файл НЕ найден по пути: {file_path}")
            file_time_str = "⚠️ Файл не знайдено (перевірте шлях)"

        # --- 3. ЧТЕНИЕ ИЗ БД И ОТПРАВКА ---
        content = db.get_latest_dmutro_report()
        
        await status_msg.delete()

        if content and content.strip():
            response_text = (
                f"📊 <b>ЗВІТ ПО РОБОТІ</b>\n"
                f"🕒 <i>Файл оновлено: {file_time_str}</i>\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"{content}"
            )
            await message.answer(response_text, parse_mode="HTML")
        else:
            await message.answer(f"📂 Скрипт спрацював ({file_time_str}), але БД повернула пустий звіт.")
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}") # Покажет ошибку в консоли
        # Пытаемся удалить сообщение о загрузке, если оно есть
        try:
            await status_msg.delete()
        except:
            pass
        await message.answer(f"❌ Критична помилка бота: {e}")


# 📋 ЗАДАЧИ ДМИТРА (Только DmutroRole.online) - ОНОВЛЕНО З ТЕРМІНОМ
@router.message(DmutroRole.online, F.text == "💳завдання,замовленя")
async def show_tasks(message: types.Message):
    await message.answer("Завантажую список завдань Дмитра...")
    
    try:
        has_tasks = False

        # 1. ТЕРМІНОВІ ЗАВДАННЯ
        srochno = db.get_dmutro_tasks("srochno_callcentr")
        if srochno:
            has_tasks = True
            await message.answer("🔴 <b>ТЕРМІНОВІ ЗАВДАННЯ:</b>", parse_mode="HTML")
            for task in srochno:
                t_id, adres, desc, date = task
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Виконати", callback_data=f"done:srochno_callcentr:{t_id}")]
                ])
                await message.answer(f"🏠 {adres}\n⚠️ {desc}\n📅 {date}", reply_markup=kb)

        # 2. ПОТОЧНІ ЗАВДАННЯ
        zadaci = db.get_dmutro_tasks("zadaci_all")
        if zadaci:
            has_tasks = True
            await message.answer("🟡 <b>ПОТОЧНІ ЗАВДАННЯ:</b>", parse_mode="HTML")
            for task in zadaci:
                t_id, adres, desc, date = task
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Виконати", callback_data=f"done:zadaci_all:{t_id}")]
                ])
                await message.answer(f"🏠 {adres}\n🛠 {desc}\n📅 {date}", reply_markup=kb)

        # 3. ЗАМОВЛЕННЯ КАРТ
        karty = db.get_dmutro_tasks("kartu_all")
        if karty:
            has_tasks = True
            await message.answer("⬜ <b>ЗАМОВЛЕННЯ КАРТ:</b>", parse_mode="HTML")
            for task in karty:
                t_id, adres, desc, date = task
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Виконати", callback_data=f"done:kartu_all:{t_id}")]
                ])
                await message.answer(f"🏠 {adres}\n💳 Клієнт: {desc}\n📅 {date}", reply_markup=kb)

        # 4. ЗАВДАННЯ З ТЕРМІНОМ (НОВЕ!)
        termin_tasks = db.get_termin_tasks("dmutro")
        if termin_tasks:
            has_tasks = True
            await message.answer("⏱ <b>ЗАВДАННЯ З ТЕРМІНОМ:</b>", parse_mode="HTML")
            for task in termin_tasks:
                t_id, adres, zavdanya, date_open, termin = task
                
                # Розрахунок залишку днів
                days_passed = (datetime.now() - date_open).days
                days_left = termin - days_passed
                
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Виконати", callback_data=f"done:zavdanya_termin:{t_id}")]
                ])
                
                await message.answer(
                    f"🏠 {adres}\n"
                    f"📝 Завдання: {zavdanya}\n"
                    f"👤 Технік: dmutro\n"
                    f"⏱ Термін: {termin} днів\n"
                    f"⏰ Залишилось: {days_left} днів\n"
                    f"📅 Відкрито: {date_open.strftime('%d.%m.%Y')}",
                    reply_markup=kb,
                    parse_mode="HTML"
                )

        if not has_tasks:
            await message.answer("🎉 У вас поки що немає активних завдань!")
            
    except Exception as e:
        await message.answer(f"❌ Помилка отримання завдань з БД: {e}")


# 🛒 СТОЛ ЗАКАЗОВ (DmutroRole.online)
@router.message(DmutroRole.online, F.text.lower() == "стіл замовлень")
async def start_stol(message: types.Message, state: FSMContext):
    await message.answer("🛒 Що потрібно замовити?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(StolZakazovState.waiting_for_item)

@router.message(StolZakazovState.waiting_for_item)
async def process_item(message: types.Message, state: FSMContext):
    try:
        db.save_stol_zakazov("dmutro", message.text)
        await message.answer("✅ Замовлення додано в базу!", reply_markup=get_dmutro_menu())
        await state.set_state(DmutroRole.online)
    except Exception as e:
        await message.answer(f"❌ Помилка запису в БД: {e}\nСпробуйте ще раз")


# 💸 ЗАТРАТЫ (DmutroRole.online)
@router.message(DmutroRole.online, F.text.lower() == "витрати")
async def start_zatratu(message: types.Message, state: FSMContext):
    await message.answer("💸 <b>Витрати</b>\nНапишіть назву витрати:", reply_markup=ReplyKeyboardRemove(), parse_mode="HTML")
    await state.set_state(ZatratuState.waiting_for_name)

@router.message(ZatratuState.waiting_for_name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("💰 Введіть суму (просто число):")
    await state.set_state(ZatratuState.waiting_for_sum)

@router.message(ZatratuState.waiting_for_sum)
async def process_sum(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.replace(",", "."))
        data = await state.get_data()
        name = data['name']
        
        db.save_zatrata("dmutro", name, amount)
        await message.answer(f"✅ Витрата '<b>{name}</b>' збережена!", parse_mode="HTML", reply_markup=get_dmutro_menu())
        await state.set_state(DmutroRole.online)
    except ValueError:
        await message.answer("❌ Це не число. Спробуйте ще раз:")
    except Exception as e:
        await message.answer(f"❌ Помилка БД: {e}")


# =======================================================
# ✅ CALLBACK: ЗАКРЫТИЕ ЗАДАЧИ
# =======================================================
@router.callback_query(F.data.startswith("done:"))
async def process_task_done(callback: types.CallbackQuery):
    """
    Обработка нажатия кнопки "Выполнить" на задаче.
    Формат: done:TABLE_NAME:TASK_ID
    """
    try:
        parts = callback.data.split(":")
        table_name = parts[1]
        task_id = parts[2]
        
        # Закрываем задачу в БД
        if db.close_task_in_db(table_name, task_id):
            # Обновляем сообщение, добавляя статус "ВИКОНАНО"
            await callback.message.edit_text(
                f"{callback.message.text}\n\n✅ <b>ВИКОНАНО</b>",
                parse_mode="HTML",
                reply_markup=None  # Убираем кнопку
            )
            await callback.answer("Статус оновлено!")
        else:
            await callback.answer("Помилка БД: Не вдалося оновити статус", show_alert=True)
    except Exception as e:
        await callback.answer(f"Помилка: {e}", show_alert=True)


# =======================================================
# 🚪 ВЫХОД
# =======================================================
@router.message(F.text.lower() == "вийти з ролі")
async def exit_dmutro(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Ви вийшли з профілю Дмитра.", reply_markup=ReplyKeyboardRemove())