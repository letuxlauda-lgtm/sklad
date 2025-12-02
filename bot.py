import asyncio
import logging
import aiohttp 
import os # <-- НОВЫЙ ИМПОРТ: для работы с переменными среды
from dotenv import load_dotenv # <-- НОВЫЙ ИМПОРТ: для чтения файла .env
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
import database as db

# --- ЗАВАНТАЖЕННЯ ЗМІННИХ СЕРЕДОВИЩА ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN") # <-- ТОКЕН БЕРЕТСЯ ИЗ ПЕРЕМЕННОЙ СРЕДЫ

# Импортируем роутеры (как в твоем файле)
from handlers_callcenter import router as callcenter_router, get_main_menu
from handlers_ruslan import router as ruslan_router
from handlers_dmutro import router as dmutro_router
from handlers_igor import router as igor_router
from handlers_finance import router as finance_router
from handlers_super import router as super_router, SuperRole, get_super_menu
from handlers_texdir import router as texdir_router, TexdirRole, get_texdir_menu 


if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не знайдено в файлі .env") # Проверка на наличие токена

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Подключаем роутеры
dp.include_router(callcenter_router)
dp.include_router(ruslan_router)
dp.include_router(dmutro_router)
dp.include_router(igor_router)
dp.include_router(finance_router)
dp.include_router(super_router)
dp.include_router(texdir_router)

# --- ФУНКЦИЯ ПОЛУЧЕНИЯ ПОГОДЫ ---
async def get_lviv_weather():
    # Координаты Львова: 49.8397, 24.0297
    url = "https://api.open-meteo.com/v1/forecast?latitude=49.8397&longitude=24.0297&daily=weather_code,temperature_2m_max,temperature_2m_min&timezone=auto"
    
    # Расшифровка кодов погоды (WMO)
    wmo_codes = {
        0: "☀️ Ясно", 1: "🌤 Преимущественно ясно", 2: "⛅️ Переменная облачность", 3: "☁️ Пасмурно",
        45: "🌫 Туман", 48: "🌫 Изморозь",
        51: "🌧 Мелкая морось", 53: "🌧 Морось", 55: "🌧 Плотная морось",
        61: "☔️ Слабый дождь", 63: "☔️ Дождь", 65: "☔️ Сильный дождь",
        71: "❄️ Слабый снег", 73: "❄️ Снег", 75: "❄️ Сильный снегопад",
        80: "⛈ Ливень", 81: "⛈ Сильный ливень", 95: "⚡️ Гроза"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                
        daily = data.get('daily', {})
        times = daily.get('time', [])
        max_temps = daily.get('temperature_2m_max', [])
        min_temps = daily.get('temperature_2m_min', [])
        codes = daily.get('weather_code', [])
        
        forecast_msg = "<b>🌦 Погода у Львові на 7 днів:</b>\n\n"
        
        # Берем 7 дней
        for i in range(min(7, len(times))):
            date_obj = datetime.strptime(times[i], "%Y-%m-%d")
            date_str = date_obj.strftime("%d.%m") # Формат 30.11
            weather_desc = wmo_codes.get(codes[i], "🤷 Нет данных")
            
            forecast_msg += (
                f"📅 <b>{date_str}</b>: {weather_desc}\n"
                f"🌡 {min_temps[i]}°C ... {max_temps[i]}°C\n\n"
            )
        return forecast_msg
        
    except Exception as e:
        logging.error(f"Ошибка получения погоды: {e}")
        return "⚠️ Не вдалося отримати прогноз погоди."

# --- START ---
@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    # Сначала отправляем приветствие
    await message.answer("👋 Привіт, я <b>бот Водолійчік</b>!")
    
    # Получаем и отправляем погоду
    weather_text = await get_lviv_weather()
    await message.answer(weather_text)
    
    # Напоминаем про суперслово
    await message.answer("Введіть <b>суперслово</b> 🔑 для авторизації.")

# --- СУПЕРСЛОВА (Вход в роли) ---
@dp.message(F.text.lower().in_({"callcentr4", "texdir1", "sup1"}))
async def role_entry(message: types.Message, state: FSMContext):
    text = message.text.lower()
    
    if text == "callcentr4":
        await state.clear()
        await message.answer("🔐 Ви увійшли як <b>Call-центр</b>", reply_markup=get_main_menu())
    
    elif text == "texdir1":
        await state.set_state(TexdirRole.online)
        await message.answer("🔐 Ви увійшли як <b>Технічний директор</b>", reply_markup=get_texdir_menu())
    
    elif text == "sup1":
        await state.set_state(SuperRole.online)
        await message.answer("😎 Добро пожаловать, Босс! Ваше меню:", reply_markup=get_super_menu())

# --- MAIN ---
async def main():
    # Инициализация таблиц БД
    db.init_tables()
    db.init_shared_tables()
    
    logging.info("🤖 Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    # Убедитесь, что у вас установлена библиотека python-dotenv
    # pip install python-dotenv
    asyncio.run(main())