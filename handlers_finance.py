import os
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
import database as db
import config

router = Router()

# --- МЕНЮ ФІНАНСИСТА ---
def get_finance_menu():
    kb = [
        [KeyboardButton(text="отримані витрати"), KeyboardButton(text="оброблені витрати")],
        [KeyboardButton(text="вийти з ролі")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- СТАН (Бейджик) ---
class FinanceRole(StatesGroup):
    online = State()

# =======================================================
# 🚪 ВХІД В РОЛЬ (Кодове слово: fin1)
# =======================================================
@router.message(F.text.lower() == "fin1")
async def finance_login(message: types.Message, state: FSMContext):
    await state.set_state(FinanceRole.online)
    await state.update_data(current_role="finance")
    await message.answer(
        "✅ Ви увійшли як <b>Фінансист</b>",
        reply_markup=get_finance_menu(),
        parse_mode="HTML"
    )

# =======================================================
# 📥 ОТРИМАНІ ВИТРАТИ
# =======================================================
@router.message(FinanceRole.online, F.text == "отримані витрати")
async def finance_received_expenses(message: types.Message):
    conn = db.get_db_connection()
    if not conn:
        await message.answer("❌ Помилка підключення до БД")
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT data_time, nazvanie, suma, texnik
            FROM zatratu
            WHERE obrabotano_financ = FALSE
            ORDER BY data_time DESC
            LIMIT 100
        """)
        
        expenses = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not expenses:
            await message.answer("📭 Немає нових витрат для обробки")
            return
        
        response = "📥 <b>ОТРИМАНІ ВИТРАТИ (необроблені)</b>\n\n"
        total = 0
        
        for expense in expenses:
            response += f"📅 {expense[0].strftime('%d.%m.%Y %H:%M')}\n"
            response += f"📝 {expense[1]}\n"
            response += f"💰 {expense[2]:.2f} грн\n"
            response += f"👤 Технік: {expense[3]}\n\n"
            total += expense[2]
        
        response += f"<b>ВСЬОГО: {total:.2f} грн</b>"
        
        await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# =======================================================
# ✅ ОБРОБЛЕНІ ВИТРАТИ
# =======================================================
@router.message(FinanceRole.online, F.text == "оброблені витрати")
async def finance_processed_expenses(message: types.Message):
    conn = db.get_db_connection()
    if not conn:
        await message.answer("❌ Помилка підключення до БД")
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT data_time, nazvanie, suma, texnik
            FROM zatratu
            WHERE obrabotano_financ = TRUE
            ORDER BY data_time DESC
            LIMIT 100
        """)
        
        expenses = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not expenses:
            await message.answer("📭 Немає оброблених витрат")
            return
        
        response = "✅ <b>ОБРОБЛЕНІ ВИТРАТИ</b>\n\n"
        total = 0
        
        for expense in expenses:
            response += f"📅 {expense[0].strftime('%d.%m.%Y %H:%M')}\n"
            response += f"📝 {expense[1]}\n"
            response += f"💰 {expense[2]:.2f} грн\n"
            response += f"👤 Технік: {expense[3]}\n\n"
            total += expense[2]
        
        response += f"<b>ВСЬОГО: {total:.2f} грн</b>"
        
        # Розбиваємо на частини, якщо занадто довго
        if len(response) > 4000:
            parts = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for part in parts:
                await message.answer(part, parse_mode="HTML")
        else:
            await message.answer(response, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Помилка: {e}")

# =======================================================
# 🚪 ВИХІД З РОЛІ
# =======================================================
@router.message(FinanceRole.online, F.text == "вийти з ролі")
async def finance_logout(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Ви вийшли з кабінету фінансиста.\n"
        "Введіть кодове слово для входу.",
        reply_markup=ReplyKeyboardRemove()
    )