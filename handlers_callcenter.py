from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import database as db

router = Router()

# =======================================================
# 🎹 КЛАВИАТУРЫ
# =======================================================

def get_main_menu():
    kb = [
        [KeyboardButton(text="⚙️нове завдання"), KeyboardButton(text="💳нове замовлення карти кл")],
        [KeyboardButton(text="☢️терміново"), KeyboardButton(text="📝статус завдань та доставок")],
        [KeyboardButton(text="ще..."), KeyboardButton(text="вихід з кабінета")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_problem_menu():
    kb = [
        [KeyboardButton(text="💰купюроприймач"), KeyboardButton(text="🪙монетоприймач")],
        [KeyboardButton(text="☠️скарга на запах"), KeyboardButton(text="🔌головний модуль")],
        [KeyboardButton(text="🚰халепа з наливом"), KeyboardButton(text="🪣збитий літраж")],
        [KeyboardButton(text="❌скасування")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_cancel_keyboard():
    """Клавіатура тільки з кнопкою скасування"""
    kb = [[KeyboardButton(text="❌скасування")]]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# =======================================================
# 🚦 СОСТОЯНИЯ (FSM)
# =======================================================

class TaskState(StatesGroup):
    waiting_for_problem = State()
    waiting_for_address = State()

class CardState(StatesGroup):
    waiting_for_name = State()
    waiting_for_address = State()

class UrgentState(StatesGroup):
    waiting_for_reason = State()
    waiting_for_address = State()


# =======================================================
# 🛠 ГЛОБАЛЬНИЙ ОБРОБНИК СКАСУВАННЯ
# =======================================================

@router.message(F.text == "❌скасування")
async def cancel_handler(message: types.Message, state: FSMContext):
    """Універсальний обробник скасування для будь-якого стану"""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("❌ Операцію скасовано.", reply_markup=get_main_menu())
    else:
        await message.answer("⚠️ Немає активної операції для скасування.", reply_markup=get_main_menu())


# =======================================================
# 1️⃣ СЦЕНАРИЙ: НОВЕ ЗАВДАННЯ
# =======================================================

@router.message(F.text == "⚙️нове завдання")
async def task_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🛠 <b>Нова задача</b>\nВиберіть проблему зі списку або напишіть свою:",
        reply_markup=get_problem_menu(),
        parse_mode="HTML"
    )
    await state.set_state(TaskState.waiting_for_problem)

@router.message(TaskState.waiting_for_problem)
async def task_problem_chosen(message: types.Message, state: FSMContext):
    text = message.text
    
    # Перевірка на скасування вже обробляється глобальним хендлером
    await state.update_data(problem=text)
    
    await message.answer(
        "📍Вкажіть <b>адресу</b> (можна неповну, я знайду)\n\n"
        "💡 Натисніть ❌скасування, щоб відмінити операцію.",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(TaskState.waiting_for_address)

@router.message(TaskState.waiting_for_address)
async def task_address_chosen(message: types.Message, state: FSMContext):
    user_addr = message.text
    
    # Перевірка на команду скасування (обробляється вище, але залишаємо для безпеки)
    if user_addr == "❌скасування":
        return
    
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer(
            "❌ Адресу не розпізнано. \n\n"
            "Спробуйте написати точніше:\n"
            "• Назва вулиці + номер будинку\n"
            "• Приклад: <code>Наукова 10</code>\n\n"
            "Або натисніть ❌скасування",
            parse_mode="HTML",
            reply_markup=get_cancel_keyboard()
        )
        return

    data = await state.get_data()
    problem_text = data['problem']
    
    # Збереження в БД з обробкою помилок
    try:
        db.save_zadaca(
            id_terem=found_obj['id_terem'],
            adres=found_obj['adress'],
            zadaca=problem_text,
            texnik=found_obj['texnik']
        )
        
        await message.answer(
            f"✅ <b>Завдання створено!</b>\n\n"
            f"🏠 Адреса: {found_obj['adress']}\n"
            f"🔧 Технік: {found_obj['texnik']}\n"
            f"📝 Проблема: {problem_text}",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ <b>Помилка збереження!</b>\n\n"
            f"Деталі: {str(e)}\n\n"
            f"Зверніться до адміністратора.",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )
        await state.clear()


# =======================================================
# 2️⃣ СЦЕНАРИЙ: ЗАКАЗ КАРТЫ
# =======================================================

@router.message(F.text == "💳нове замовлення карти кл")
async def card_start(message: types.Message, state: FSMContext):
    await message.answer(
        "💳 <b>Замовлення карти</b>\n\n"
        "Вкажіть ім'я клієнта:\n"
        "💡 Натисніть ❌скасування для відміни",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(CardState.waiting_for_name)

@router.message(CardState.waiting_for_name)
async def card_name_entered(message: types.Message, state: FSMContext):
    await state.update_data(client_name=message.text)
    await message.answer(
        "📍 Вкажіть адресу доставки\n"
        "(можна з помилками, я зрозумію)\n\n"
        "💡 Або ❌скасування для відміни",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(CardState.waiting_for_address)

@router.message(CardState.waiting_for_address)
async def card_address_entered(message: types.Message, state: FSMContext):
    user_addr = message.text
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer(
            "❌ Адресу не знайдено.\n\n"
            "Уточніть (наприклад: <code>Наукова 10</code>)\n"
            "Або натисніть ❌скасування",
            parse_mode="HTML",
            reply_markup=get_cancel_keyboard()
        )
        return

    data = await state.get_data()
    client_name = data['client_name']

    try:
        db.save_kartu(
            id_terem=found_obj['id_terem'],
            adres=found_obj['adress'],
            kartu=client_name,
            texnik=found_obj['texnik']
        )

        await message.answer(
            f"✅ <b>Замовлення на карту створено!</b>\n\n"
            f"👤 Клієнт: {client_name}\n"
            f"🏠 Адреса: {found_obj['adress']}\n"
            f"🔧 Технік: {found_obj['texnik']}",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ Помилка збереження замовлення!\n{str(e)}",
            reply_markup=get_main_menu()
        )
        await state.clear()


# =======================================================
# 3️⃣ СЦЕНАРИЙ: СРОЧНО
# =======================================================

@router.message(F.text == "☢️терміново")
async def urgent_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🔥 <b>ТЕРМІНОВО</b>\n\n"
        "Вкажіть причину (що сталося?):\n"
        "💡 Або ❌скасування для відміни",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(UrgentState.waiting_for_reason)

@router.message(UrgentState.waiting_for_reason)
async def urgent_reason_entered(message: types.Message, state: FSMContext):
    await state.update_data(reason=message.text)
    await message.answer(
        "📍 Вкажіть адресу:\n"
        "💡 Або ❌скасування для відміни",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UrgentState.waiting_for_address)

@router.message(UrgentState.waiting_for_address)
async def urgent_address_entered(message: types.Message, state: FSMContext):
    user_addr = message.text
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer(
            "❌ Адресу не знайдено.\n"
            "Спробуйте ще раз або натисніть ❌скасування",
            reply_markup=get_cancel_keyboard()
        )
        return

    data = await state.get_data()
    reason = data['reason']

    try:
        db.save_srochno(
            id_terem=found_obj['id_terem'],
            adres=found_obj['adress'],
            srocno=reason,
            texnik=found_obj['texnik']
        )

        await message.answer(
            f"🔥 <b>Термінове завдання створено!</b>\n\n"
            f"⚠️ Причина: {reason}\n"
            f"🏠 Адреса: {found_obj['adress']}\n"
            f"🔧 Технік: {found_obj['texnik']}",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"❌ Помилка збереження термінового завдання!\n{str(e)}",
            reply_markup=get_main_menu()
        )
        await state.clear()


# =======================================================
# 4️⃣ СТАТУС ЗАВДАНЬ ТА ДОСТАВОК
# =======================================================

@router.message(F.text == "📝статус завдань та доставок")
async def show_status_and_analytics(message: types.Message):
    """Показує аналітику та статус завдань"""
    
    try:
        # --- ПОВТОРНІ ПОЛОМКИ ---
        recurring = db.get_recurring_issues()
        rec_report = "🔄 <b>Повторні поломки (&gt;2 рази за 30 днів):</b>\n\n"

        if recurring:
            for row in recurring:
                terem_id, count, adr, tex = row
                rec_report += f"⚠️ <b>{count} раз(а)</b>: {adr} (ID:{terem_id}) [{tex}]\n"
        else:
            rec_report += "✅ Повторів немає"

        await message.answer(rec_report, parse_mode="HTML")

        # --- АКТИВНІ ЗАВДАННЯ ---
        await message.answer("⏳ <b>Активні завдання (open):</b>\n", parse_mode="HTML")
        
        active_tasks = db.get_all_open_tasks("zadaci_all")
        if active_tasks:
            for row in active_tasks:
                try:
                    task_id, adres, problem, date_open, texnik = row
                    await message.answer(
                        f"🔧 <b>ID:</b> {task_id}\n"
                        f"📍 <b>Адреса:</b> {adres}\n"
                        f"❗ <b>Проблема:</b> {problem}\n"
                        f"👤 <b>Технік:</b> {texnik}",
                        parse_mode="HTML"
                    )
                except ValueError as e:
                    await message.answer(f"⚠️ Помилка структури даних: {row}")
        else:
            await message.answer("✅ Активних завдань немає", parse_mode="HTML")

        # --- АКТИВНІ ЗАМОВЛЕННЯ НА КАРТИ ---
        await message.answer("💳 <b>Активні замовлення карт (open):</b>\n", parse_mode="HTML")
        
        active_cards = db.get_all_open_tasks("kartu_all")
        if active_cards:
            for row in active_cards:
                try:
                    card_id, adres, client, date_open, texnik = row
                    await message.answer(
                        f"💳 <b>ID:</b> {card_id}\n"
                        f"📍 <b>Адреса:</b> {adres}\n"
                        f"👤 <b>Клієнт:</b> {client}\n"
                        f"🔧 <b>Технік:</b> {texnik}",
                        parse_mode="HTML"
                    )
                except ValueError:
                    await message.answer(f"⚠️ Помилка: {row}")
        else:
            await message.answer("✅ Активних замовлень немає", parse_mode="HTML")

        await message.answer("✅ Звіт завершено", reply_markup=get_main_menu())
        
    except Exception as e:
        await message.answer(
            f"❌ <b>Помилка отримання статусу!</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=get_main_menu()
        )


# =======================================================
# 5️⃣ ЩЕ...
# =======================================================

@router.message(F.text == "ще...")
async def show_more(message: types.Message):
    await message.answer(
        "🔧 <b>Додаткові функції</b>\n\n"
        "Ця секція в розробці.\n"
        "Тут будуть додаткові інструменти для Call-центру.",
        parse_mode="HTML",
        reply_markup=get_main_menu()
    )


# =======================================================
# 6️⃣ ВИХІД З КАБІНЕТУ
# =======================================================

@router.message(F.text == "вихід з кабінета")
async def exit_role(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Ви вийшли з кабінету Call-центру.\n"
        "Введіть суперслово для входу.",
        reply_markup=ReplyKeyboardRemove()
    )