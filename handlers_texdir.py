import os
import html
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
import database as db

router = Router()

# --- МЕНЮ ТЕХДИРЕКТОРА ---
def get_texdir_menu():
    kb = [
        [KeyboardButton(text="⚙️нове завдання"), KeyboardButton(text="💳нове замовлення карти кл")],
        [KeyboardButton(text="📝завдання с терміном"), KeyboardButton(text="🛒стіл замовлень")],
        [KeyboardButton(text="💰витрати"), KeyboardButton(text="📝завдання та замовлення")],
        [KeyboardButton(text="вийти з ролі")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_problem_menu():
    kb = [
        [KeyboardButton(text="💰купюроприймач"), KeyboardButton(text="🪙монетоприймач")],
        [KeyboardButton(text="☠️скарга на запах"), KeyboardButton(text="🔌головний модуль")],
        [KeyboardButton(text="🚰халепа з наливом"), KeyboardButton(text="🪣збитий літраж")],
        [KeyboardButton(text="🔙назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- СТАНИ ТЕХДИРЕКТОРА ---
class TexdirRole(StatesGroup):
    online = State()

class TexdirTaskState(StatesGroup):
    waiting_for_problem = State()
    waiting_for_address = State()

class TexdirCardState(StatesGroup):
    waiting_for_name = State()
    waiting_for_address = State()

class TexdirZatratuState(StatesGroup):
    waiting_for_name = State()
    waiting_for_sum = State()

# НОВИЙ СТАН ДЛЯ ЗАВДАНЬ З ТЕРМІНОМ
class TexdirTerminTaskState(StatesGroup):
    waiting_for_task = State()
    waiting_for_termin = State()
    waiting_for_address = State()

# 🚪 ВХІД В РОЛЬ (texdir)
@router.message(F.text.lower() == "texdir")
async def texdir_login(message: types.Message, state: FSMContext):
    await state.set_state(TexdirRole.online)
    await state.update_data(current_role="texdir")
    await message.answer(
        "✅ Ви увійшли як <b>Техдиректор</b>",
        reply_markup=get_texdir_menu(),
        parse_mode="HTML"
    )

# ⚙️ НОВЕ ЗАВДАННЯ
@router.message(TexdirRole.online, F.text == "⚙️нове завдання")
async def texdir_new_task_start(message: types.Message, state: FSMContext):
    await state.set_state(TexdirTaskState.waiting_for_problem)
    await state.update_data(urgent=False)
    await message.answer(
        "🔧 Оберіть або введіть проблему:",
        reply_markup=get_problem_menu()
    )

@router.message(TexdirTaskState.waiting_for_problem)
async def texdir_task_problem(message: types.Message, state: FSMContext):
    if message.text == "🔙назад":
        await state.set_state(TexdirRole.online)
        await message.answer("❌ Скасовано", reply_markup=get_texdir_menu())
        return
    
    problem = message.text
    await state.update_data(problem=problem)
    await state.set_state(TexdirTaskState.waiting_for_address)
    await message.answer(
        "📍 Введіть адресу:",
        reply_markup=ReplyKeyboardRemove()
    )

@router.message(TexdirTaskState.waiting_for_address)
async def texdir_task_address(message: types.Message, state: FSMContext):
    address = message.text
    data = await state.get_data()
    problem = data.get('problem')
    urgent = data.get('urgent', False)
    
    address_info = db.search_terem_info(address)
    
    if not address_info:
        await message.answer(
            "⚠️ Адресу не знайдено в базі.\n\n"
            "Введіть адресу ще раз:"
        )
        return
    
    texnik = address_info['texnik']
    id_terem = address_info['id_terem']
    
    success = db.save_zadaca(
        id_terem=id_terem,
        adres=address_info['adress'],
        zadaca=problem,
        texnik=texnik
    )
    
    if success:
        await message.answer(
            f"✅ Завдання створено!\n\n"
            f"❗ Проблема: {problem}\n"
            f"📍 Адреса: {address_info['adress']}\n"
            f"👤 Технік: {texnik}",
            reply_markup=get_texdir_menu()
        )
    else:
        await message.answer(
            "❌ Помилка створення завдання",
            reply_markup=get_texdir_menu()
        )
    
    await state.set_state(TexdirRole.online)

# 📝 НОВЕ ЗАВДАННЯ З ТЕРМІНОМ
@router.message(TexdirRole.online, F.text == "📝завдання с терміном")
async def texdir_termin_task_start(message: types.Message, state: FSMContext):
    await state.set_state(TexdirTerminTaskState.waiting_for_task)
    await message.answer(
        "📝 Вкажіть саме завдання:",
        reply_markup=ReplyKeyboardRemove()
    )

@router.message(TexdirTerminTaskState.waiting_for_task)
async def texdir_termin_task_name(message: types.Message, state: FSMContext):
    task_text = message.text
    await state.update_data(task_text=task_text)
    await state.set_state(TexdirTerminTaskState.waiting_for_termin)
    await message.answer(
        "⏱ Який термін на завдання? Вкажіть в днях (наприклад: 7):"
    )

@router.message(TexdirTerminTaskState.waiting_for_termin)
async def texdir_termin_days(message: types.Message, state: FSMContext):
    try:
        termin_days = int(message.text)
        if termin_days <= 0:
            await message.answer("❌ Термін має бути більше 0. Спробуйте ще раз:")
            return
        
        await state.update_data(termin_days=termin_days)
        await state.set_state(TexdirTerminTaskState.waiting_for_address)
        await message.answer("📍 Вкажіть адресу:")
        
    except ValueError:
        await message.answer("❌ Це не число. Введіть кількість днів (наприклад: 7):")

@router.message(TexdirTerminTaskState.waiting_for_address)
async def texdir_termin_address(message: types.Message, state: FSMContext):
    address = message.text
    data = await state.get_data()
    task_text = data.get('task_text')
    termin_days = data.get('termin_days')
    
    address_info = db.search_terem_info(address)
    
    if not address_info:
        await message.answer(
            "⚠️ Адресу не знайдено в базі.\n\n"
            "Введіть адресу ще раз:"
        )
        return
    
    texnik = address_info['texnik']
    id_terem = address_info['id_terem']
    
    success = db.save_termin_task(
        id_terem=id_terem,
        adres=address_info['adress'],
        zavdanya=task_text,
        termin=termin_days,
        texnik=texnik
    )
    
    if success:
        await message.answer(
            f"✅ Завдання з терміном створено!\n\n"
            f"📝 Завдання: {task_text}\n"
            f"📍 Адреса: {address_info['adress']}\n"
            f"👤 Технік: {texnik}\n"
            f"⏱ Термін: {termin_days} днів",
            reply_markup=get_texdir_menu()
        )
    else:
        await message.answer(
            "❌ Помилка створення завдання",
            reply_markup=get_texdir_menu()
        )
    
    await state.set_state(TexdirRole.online)

# 💳 НОВЕ ЗАМОВЛЕННЯ КАРТИ
@router.message(TexdirRole.online, F.text == "💳нове замовлення карти кл")
async def texdir_new_card_start(message: types.Message, state: FSMContext):
    await state.set_state(TexdirCardState.waiting_for_name)
    await message.answer(
        "📝 Введіть ім'я клієнта:",
        reply_markup=ReplyKeyboardRemove()
    )

@router.message(TexdirCardState.waiting_for_name)
async def texdir_card_name(message: types.Message, state: FSMContext):
    name = message.text
    await state.update_data(client_name=name)
    await state.set_state(TexdirCardState.waiting_for_address)
    await message.answer("📍 Введіть адресу:")

@router.message(TexdirCardState.waiting_for_address)
async def texdir_card_address(message: types.Message, state: FSMContext):
    address = message.text
    data = await state.get_data()
    client_name = data.get('client_name')
    
    address_info = db.search_terem_info(address)
    
    if not address_info:
        await message.answer(
            "⚠️ Адресу не знайдено в базі.\n\n"
            "Введіть адресу ще раз:"
        )
        return
    
    texnik = address_info['texnik']
    id_terem = address_info['id_terem']
    
    success = db.save_kartu(
        id_terem=id_terem,
        adres=address_info['adress'],
        kartu=client_name,
        texnik=texnik
    )
    
    if success:
        await message.answer(
            f"✅ Замовлення картки створено!\n\n"
            f"👤 Клієнт: {client_name}\n"
            f"📍 Адреса: {address_info['adress']}\n"
            f"🔧 Технік: {texnik}",
            reply_markup=get_texdir_menu()
        )
    else:
        await message.answer(
            "❌ Помилка створення замовлення",
            reply_markup=get_texdir_menu()
        )
    
    await state.set_state(TexdirRole.online)

# 📝 ЗАВДАННЯ ТА ЗАМОВЛЕННЯ
@router.message(TexdirRole.online, F.text == "📝завдання та замовлення")
async def texdir_tasks_and_orders(message: types.Message):
    conn = db.get_connection()
    if not conn:
        await message.answer("❌ Помилка підключення до БД")
        return
    
    try:
        cursor = conn.cursor()
        
        # Звичайні завдання
        cursor.execute("""
            SELECT date_time_open, zadaca, adres, texnik, status
            FROM zadaci_all
            WHERE status = 'open'
            ORDER BY date_time_open DESC
            LIMIT 50
        """)
        tasks = cursor.fetchall()
        
        # Завдання з терміном
        cursor.execute("""
            SELECT date_time_open, zavdanya, adres, texnik, termin, status
            FROM zavdanya_termin
            WHERE status = 'open'
            ORDER BY date_time_open DESC
            LIMIT 50
        """)
        termin_tasks = cursor.fetchall()
        
        # Замовлення карток
        cursor.execute("""
            SELECT date_time_open, kartu, adres, texnik, status
            FROM kartu_all
            WHERE status = 'open'
            ORDER BY date_time_open DESC
            LIMIT 50
        """)
        cards = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        response = "📋 <b>ЗАВДАННЯ ТА ЗАМОВЛЕННЯ</b>\n\n"
        
        if tasks:
            response += "🔧 <b>Завдання:</b>\n"
            for task in tasks:
                response += f"📅 {task[0].strftime('%d.%m %H:%M')}\n"
                response += f"❗ {task[1]}\n"
                response += f"📍 {task[2]}\n"
                response += f"👤 {task[3]}\n"
                response += f"⚡ {task[4]}\n\n"
        else:
            response += "✅ Немає активних завдань\n\n"
        
        if termin_tasks:
            response += "⏱ <b>Завдання з терміном:</b>\n"
            for task in termin_tasks:
                from datetime import datetime
                days_passed = (datetime.now() - task[0]).days
                days_left = task[4] - days_passed
                response += f"📅 {task[0].strftime('%d.%m %H:%M')}\n"
                response += f"📝 {task[1]}\n"
                response += f"📍 {task[2]}\n"
                response += f"👤 {task[3]}\n"
                response += f"⏱ Термін: {task[4]} днів, залишилось: {days_left} днів\n"
                response += f"⚡ {task[5]}\n\n"
        else:
            response += "✅ Немає завдань з терміном\n\n"
        
        if cards:
            response += "💳 <b>Замовлення карток:</b>\n"
            for card in cards:
                response += f"📅 {card[0].strftime('%d.%m %H:%M')}\n"
                response += f"👤 {card[1]}\n"
                response += f"📍 {card[2]}\n"
                response += f"🔧 {card[3]}\n"
                response += f"⚡ {card[4]}\n\n"
        else:
            response += "✅ Немає замовлень карток\n"
        
        # Розбиваємо на частини, якщо занадто довго
        if len(response) > 4000:
            parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# 🛒 СТІЛ ЗАМОВЛЕНЬ
@router.message(TexdirRole.online, F.text == "🛒стіл замовлень")
async def texdir_stol_zakazov(message: types.Message):
    conn = db.get_connection()
    if not conn:
        await message.answer("❌ Помилка підключення до БД")
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT date_time_open, zakaz, texnik, status
            FROM stol_zakazov
            WHERE status = 'open'
            ORDER BY date_time_open DESC
            LIMIT 50
        """)
        items = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not items:
            await message.answer("📭 Стіл замовлень порожній")
            return
        
        response = "🛒 <b>СТІЛ ЗАМОВЛЕНЬ</b>\n\n"
        
        for item in items:
            response += f"📅 {item[0].strftime('%d.%m %H:%M')}\n"
            response += f"📦 {item[1]}\n"
            response += f"👤 Замовив: {item[2]}\n"
            response += f"⚡ {item[3]}\n\n"
        
        if len(response) > 4000:
            parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# 💰 ВИТРАТИ
@router.message(TexdirRole.online, F.text == "💰витрати")
async def texdir_expenses(message: types.Message):
    conn = db.get_connection()
    if not conn:
        await message.answer("❌ Помилка підключення до БД")
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, texnik, zatrata, suma_zatrat, status
            FROM zatratu_all
            ORDER BY id DESC
            LIMIT 50
        """)
        expenses = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not expenses:
            await message.answer("📭 Немає витрат")
            return
        
        response = "💰 <b>ВИТРАТИ</b>\n\n"
        total = 0
        
        for expense in expenses:
            status_icon = "✅" if expense[4] == 'closed' else "⏳"
            response += f"{status_icon} ID: {expense[0]}\n"
            response += f"📝 {expense[2]}\n"
            response += f"💵 {expense[3]:.2f} грн\n"
            response += f"👤 {expense[1]}\n\n"
            total += expense[3]
        
        response += f"<b>ВСЬОГО: {total:.2f} грн</b>"
        
        if len(response) > 4000:
            parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# 🚪 ВИЙТИ З РОЛІ
@router.message(TexdirRole.online, F.text == "вийти з ролі")
async def texdir_logout(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Ви вийшли з кабінету техдиректора.\n"
        "Введіть кодове слово для входу.",
        reply_markup=ReplyKeyboardRemove()
    )