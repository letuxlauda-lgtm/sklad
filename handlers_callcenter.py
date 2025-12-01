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
    if text == "❌скасування":
        await state.clear()
        await message.answer("❌ Скасовано.", reply_markup=get_main_menu())
        return

    await state.update_data(problem=text)
    
    await message.answer(
        "📍Вкажіть <b>адресу</b> (можна неповну, я знайду)",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await state.set_state(TaskState.waiting_for_address)

@router.message(TaskState.waiting_for_address)
async def task_address_chosen(message: types.Message, state: FSMContext):
    user_addr = message.text
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer("❌ Адресу не розпізнано. Спробуйте написати точніше (Вулиця, номер):")
        return

    data = await state.get_data()
    problem_text = data['problem']
    
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


# =======================================================
# 2️⃣ СЦЕНАРИЙ: ЗАКАЗ КАРТЫ
# =======================================================

@router.message(F.text == "💳нове замовлення карти кл")
async def card_start(message: types.Message, state: FSMContext):
    await message.answer(
        "💳 <b>Замовлення карти</b>\nВкажіть ім'я клієнта:",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await state.set_state(CardState.waiting_for_name)

@router.message(CardState.waiting_for_name)
async def card_name_entered(message: types.Message, state: FSMContext):
    await state.update_data(client_name=message.text)
    await message.answer("📍 Вкажіть адресу доставки (можна з помилками, я зрозумію):")
    await state.set_state(CardState.waiting_for_address)

@router.message(CardState.waiting_for_address)
async def card_address_entered(message: types.Message, state: FSMContext):
    user_addr = message.text
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer("❌ Адресу не знайдено. Уточніть (наприклад: 'Наукова 10'):")
        return

    data = await state.get_data()
    client_name = data['client_name']

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


# =======================================================
# 3️⃣ СЦЕНАРИЙ: СРОЧНО
# =======================================================

@router.message(F.text == "☢️терміново")
async def urgent_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🔥 <b>ТЕРМІНОВО</b>\nВкажіть причину (що сталося?):",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await state.set_state(UrgentState.waiting_for_reason)

@router.message(UrgentState.waiting_for_reason)
async def urgent_reason_entered(message: types.Message, state: FSMContext):
    await state.update_data(reason=message.text)
    await message.answer("📍 Вкажіть адресу:")
    await state.set_state(UrgentState.waiting_for_address)

@router.message(UrgentState.waiting_for_address)
async def urgent_address_entered(message: types.Message, state: FSMContext):
    user_addr = message.text
    found_obj = db.search_terem_info(user_addr)
    
    if not found_obj:
        await message.answer("❌ Адресу не знайдено. Спробуйте ще раз:")
        return

    data = await state.get_data()
    reason = data['reason']

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


# =======================================================
# 4️⃣ СТАТУС ЗАВДАНЬ ТА ДОСТАВОК
# =======================================================

@router.message(F.text == "📝статус завдань та доставок")
async def show_status_and_analytics(message: types.Message):
    """Показує аналітику та статус завдань"""

    # --- ПОВТОРНІ ПОЛОМКИ ---
    recurring = db.get_recurring_issues()
    rec_report = "🔄 <b>Повторні поломки (>2 рази за 30 днів):</b>\n"

    if recurring:
        for row in recurring:
            terem_id, count, adr, tex = row
            rec_report += f"⚠️ <b>{count} раз(а)</b>: {adr} (ID:{terem_id}) [{tex}]\n"
    else:
        rec_report += "Повторів немає"

    await message.answer(rec_report, parse_mode="HTML")

    # --- АКТИВНІ ЗАВДАННЯ ---
    await message.answer("⏳ <b>Активні завдання (open):</b>\n", parse_mode="HTML")
    
    active_tasks = db.get_all_open_tasks("zadaci_all")
    if active_tasks:
        for row in active_tasks:
            # Убедитесь, что количество переменных совпадает с тем, что возвращает БД
            try:
                task_id, adres, problem, date_open, texnik = row
                await message.answer(
                    f"🔧 ID: {task_id}\n"
                    f"📍 {adres}\n"
                    f"❗ {problem}\n"
                    f"👤 Технік: {texnik}",
                    parse_mode="HTML"
                )
            except ValueError:
                await message.answer(f"🔧 Запис: {row} (Помилка структури даних)")
    else:
        await message.answer("Активних завдань немає ✅", parse_mode="HTML")

    # --- АКТИВНІ ЗАМОВЛЕННЯ НА КАРТИ ---
    await message.answer("💳 <b>Активні замовлення карт (open):</b>\n", parse_mode="HTML")
    
    active_cards = db.get_all_open_tasks("kartu_all")
    if active_cards:
        for row in active_cards:
            try:
                card_id, adres, client, date_open, texnik = row
                await message.answer(
                    f"💳 ID: {card_id}\n"
                    f"📍 {adres}\n"
                    f"👤 Клієнт: {client}\n"
                    f"🔧 Технік: {texnik}",
                    parse_mode="HTML"
                )
            except ValueError:
                 await message.answer(f"💳 Запис: {row}")
    else:
        await message.answer("Активних замовлень немає ✅", parse_mode="HTML")

    # Я убрал ошибочную строку "Активних термінів немає", которая вызывала IndentationError,
    # так как здесь нет кода для проверки срочных задач (srochno).

    await message.answer("✅ Звіт завершено", reply_markup=get_main_menu())


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