import psycopg2
import logging
from datetime import datetime
from thefuzz import process, fuzz # Библиотека для нечеткого поиска

# --- КОНФИГУРАЦИЯ БД ---
DB_HOST = "49.13.142.186"
DB_PORT = "5432"
DB_NAME = "datavodolij"
DB_USER = "dataanalyst"
DB_PASSWORD = "))vodoliJuser2025"

# --- ВАШ СПИСОК АДРЕСОВ (Очищенный и исправленный) ---
ADDRESS_DB = [
    {'id_terem': 153, 'adress': 'Антонича, 6', 'texnik': 'ruslan'},
    {'id_terem': 240, 'adress': 'Багряного, 39', 'texnik': 'ruslan'},
    {'id_terem': 297, 'adress': 'Біберовича, 11', 'texnik': 'ruslan'},
    {'id_terem': 236, 'adress': 'Брюховицька, 143', 'texnik': 'ruslan'},
    {'id_terem': 156, 'adress': 'Брюховичі Івасюка, 1', 'texnik': 'ruslan'},
    {'id_terem': 243, 'adress': 'Брюховичі Львівська, 92', 'texnik': 'ruslan'},
    {'id_terem': 254, 'adress': 'Вашингтона, 4в', 'texnik': 'ruslan'},
    {'id_terem': 202, 'adress': 'Виговського, 5', 'texnik': 'ruslan'},
    {'id_terem': 52, 'adress': 'Виговського, 5б', 'texnik': 'ruslan'},
    {'id_terem': 178, 'adress': 'Генерала Тарнавського, 104б', 'texnik': 'ruslan'},
    {'id_terem': 305, 'adress': 'Гориня, 39', 'texnik': 'ruslan'},
    {'id_terem': 212, 'adress': 'Городоцька, 213', 'texnik': 'ruslan'},
    {'id_terem': 269, 'adress': 'Городоцька, 226а', 'texnik': 'ruslan'},
    {'id_terem': 114, 'adress': 'Демнянська, 26', 'texnik': 'ruslan'},
    {'id_terem': 226, 'adress': 'Дністерська, 1', 'texnik': 'ruslan'},
    {'id_terem': 87, 'adress': 'Довженка, 5', 'texnik': 'ruslan'},
    {'id_terem': 118, 'adress': 'Драгана, 4б', 'texnik': 'ruslan'},
    {'id_terem': 108, 'adress': 'Дунайська, 7', 'texnik': 'ruslan'},
    {'id_terem': 165, 'adress': 'Зелена, 204', 'texnik': 'ruslan'},
    {'id_terem': 280, 'adress': 'Зелена, 44', 'texnik': 'ruslan'},
    {'id_terem': 57, 'adress': 'Зимна Вода, Тичини, 9', 'texnik': 'ruslan'},
    {'id_terem': 282, 'adress': 'Йосифа Сліпого, 22', 'texnik': 'ruslan'},
    {'id_terem': 242, 'adress': 'Караджича, 29б', 'texnik': 'ruslan'},
    {'id_terem': 336, 'adress': 'Кубійовича, 31', 'texnik': 'ruslan'},
    {'id_terem': 184, 'adress': 'Кульпарківська, 135', 'texnik': 'ruslan'},
    {'id_terem': 109, 'adress': 'Кульпарківська, 230', 'texnik': 'ruslan'},
    {'id_terem': 335, 'adress': 'Лазаренка, 1', 'texnik': 'ruslan'},
    {'id_terem': 292, 'adress': 'Лапаївка, Геофізиків, 17', 'texnik': 'ruslan'},
    {'id_terem': 54, 'adress': 'Левицького, 43а', 'texnik': 'ruslan'},
    {'id_terem': 85, 'adress': 'Левицького, 106', 'texnik': 'ruslan'},
    {'id_terem': 232, 'adress': 'Липова алея, 1', 'texnik': 'ruslan'},
    {'id_terem': 203, 'adress': 'Медової печери, 65', 'texnik': 'ruslan'},
    {'id_terem': 60, 'adress': 'Мечнікова, 16е', 'texnik': 'ruslan'},
    {'id_terem': 298, 'adress': 'Освицька, 1', 'texnik': 'ruslan'},
    {'id_terem': 281, 'adress': 'Пасічна, 84а', 'texnik': 'ruslan'},
    {'id_terem': 227, 'adress': 'Пасічна, 171', 'texnik': 'ruslan'},
    {'id_terem': 208, 'adress': 'Петлюри, 2а', 'texnik': 'ruslan'},
    {'id_terem': 314, 'adress': 'Пулюя, 29', 'texnik': 'ruslan'},
    {'id_terem': 279, 'adress': 'Пулюя, 40', 'texnik': 'ruslan'},
    {'id_terem': 53, 'adress': 'Родини Крушельницьких, 1а', 'texnik': 'ruslan'},
    {'id_terem': 296, 'adress': 'Садівнича, 27', 'texnik': 'ruslan'},
    {'id_terem': 183, 'adress': 'Скорини, 44', 'texnik': 'ruslan'},
    {'id_terem': 302, 'adress': 'Сокільники, Г.Сковороди, 56', 'texnik': 'ruslan'},
    {'id_terem': 217, 'adress': 'Сокільники, Героїв Майдану, 17в', 'texnik': 'ruslan'},
    {'id_terem': 244, 'adress': 'Стрийська, 45в', 'texnik': 'ruslan'},
    {'id_terem': 127, 'adress': 'Стрийська, 51', 'texnik': 'ruslan'},
    {'id_terem': 316, 'adress': 'Стрийська, 108', 'texnik': 'ruslan'},
    {'id_terem': 56, 'adress': 'Тернопільська, 21', 'texnik': 'ruslan'},
    {'id_terem': 174, 'adress': 'Тернопільська, 8', 'texnik': 'ruslan'},
    {'id_terem': 200, 'adress': 'Трускавецька, 129', 'texnik': 'ruslan'},
    {'id_terem': 155, 'adress': 'Угорська, 12', 'texnik': 'ruslan'},
    {'id_terem': 206, 'adress': 'Угорська, 14б', 'texnik': 'ruslan'},
    {'id_terem': 104, 'adress': 'Шевченка, 111', 'texnik': 'ruslan'},
    {'id_terem': 211, 'adress': 'Яворницького, 8', 'texnik': 'ruslan'},
    {'id_terem': 249, 'adress': 'Віденська, 9', 'texnik': 'ruslan'},
    {'id_terem': 277, 'adress': 'Кавалерідзе, 23', 'texnik': 'ruslan'},
    {'id_terem': 58, 'adress': 'Куровця, 36', 'texnik': 'ruslan'},
    {'id_terem': 311, 'adress': 'Коломийська, 7', 'texnik': 'ruslan'},
    {'id_terem': 163, 'adress': 'Левицького, 15', 'texnik': 'ruslan'},
    {'id_terem': 164, 'adress': 'Бандери, 69', 'texnik': 'igor'},
    {'id_terem': 327, 'adress': 'Веливока, 9', 'texnik': 'igor'},
    {'id_terem': 205, 'adress': 'Винники, Винна гора, 10б', 'texnik': 'igor'},
    {'id_terem': 126, 'adress': 'Винники, Сахарова, 10', 'texnik': 'igor'},
    {'id_terem': 251, 'adress': 'Винники, Франка, 53', 'texnik': 'igor'},
    {'id_terem': 154, 'adress': 'Гайдамацька, 9а', 'texnik': 'igor'},
    {'id_terem': 268, 'adress': 'Городоцька, 45', 'texnik': 'igor'},
    {'id_terem': 51, 'adress': 'Грінченка, 6', 'texnik': 'igor'},
    {'id_terem': 195, 'adress': 'Грушевського, 7/9', 'texnik': 'igor'},
    {'id_terem': 55, 'adress': 'Довбуша, 1', 'texnik': 'igor'},
    {'id_terem': 225, 'adress': 'Замарстинівська, 55г', 'texnik': 'igor'},
    {'id_terem': 180, 'adress': 'Замарстинівська, 170б', 'texnik': 'igor'},
    {'id_terem': 258, 'adress': 'Замарстинівська, 170н', 'texnik': 'igor'},
    {'id_terem': 172, 'adress': 'Зарицьких, 5', 'texnik': 'igor'},
    {'id_terem': 326, 'adress': 'Зелена, 17', 'texnik': 'igor'},
    {'id_terem': 239, 'adress': 'Князя Романа, 9', 'texnik': 'igor'},
    {'id_terem': 230, 'adress': 'Котика, 9', 'texnik': 'igor'},
    {'id_terem': 233, 'adress': 'Липинського, 29', 'texnik': 'igor'},
    {'id_terem': 231, 'adress': 'Лисиничі, Шухевича, 5', 'texnik': 'igor'},
    {'id_terem': 193, 'adress': 'Личаківська, 4/6', 'texnik': 'igor'},
    {'id_terem': 157, 'adress': 'Личаківська, 70а', 'texnik': 'igor'},
    {'id_terem': 286, 'adress': 'Личаківська, 86', 'texnik': 'igor'},
    {'id_terem': 186, 'adress': 'Личаківська, 163', 'texnik': 'igor'},
    {'id_terem': 328, 'adress': 'Мазепи, 26', 'texnik': 'igor'},
    {'id_terem': 198, 'adress': 'Малоголосківська, 16', 'texnik': 'igor'},
    {'id_terem': 188, 'adress': 'Миколайчука, 4а', 'texnik': 'igor'},
    {'id_terem': 61, 'adress': 'Наливайка, 20', 'texnik': 'igor'},
    {'id_terem': 196, 'adress': 'Ніжинська, 16', 'texnik': 'igor'},
    {'id_terem': 59, 'adress': 'Очеретяна, 10', 'texnik': 'igor'},
    {'id_terem': 119, 'adress': 'Пекарська, 14', 'texnik': 'igor'},
    {'id_terem': 238, 'adress': 'Під Голоском, 24б', 'texnik': 'igor'},
    {'id_terem': 86, 'adress': 'просп. Свободи, 1/3', 'texnik': 'igor'},
    {'id_terem': 218, 'adress': 'просп.В.Чорновола, 7а', 'texnik': 'igor'},
    {'id_terem': 264, 'adress': 'просп.В.Чорновола, 55', 'texnik': 'igor'},
    {'id_terem': 192, 'adress': 'просп.В.Чорновола, 67ж', 'texnik': 'igor'},
    {'id_terem': 124, 'adress': 'просп.В.Чорновола, 69', 'texnik': 'igor'},
    {'id_terem': 113, 'adress': 'просп.В.Чорновола, 101', 'texnik': 'igor'},
    {'id_terem': 12, 'adress': 'Січових Стрільців, 13', 'texnik': 'igor'},
    {'id_terem': 122, 'adress': 'Тичини, 14', 'texnik': 'igor'},
    {'id_terem': 319, 'adress': 'Тракт Глинянський, 163', 'texnik': 'igor'},
    {'id_terem': 112, 'adress': 'Франка, 69', 'texnik': 'igor'},
    {'id_terem': 246, 'adress': 'Хмельницького, 257', 'texnik': 'igor'},
    {'id_terem': 185, 'adress': 'Хмельницького, 76', 'texnik': 'igor'},
    {'id_terem': 123, 'adress': 'Щурата, 9', 'texnik': 'igor'},
    {'id_terem': 283, 'adress': 'Під Дубом, 17', 'texnik': 'igor'},
    {'id_terem': 322, 'adress': 'Шолом-Алейхема, 20', 'texnik': 'igor'},
    {'id_terem': 107, 'adress': 'Кошиця 1', 'texnik': 'igor'},
    {'id_terem': 190, 'adress': 'Братів Міхновських, 23', 'texnik': 'dmutro'},
    {'id_terem': 179, 'adress': 'В.Великого, 1', 'texnik': 'dmutro'},
    {'id_terem': 116, 'adress': 'В.Великого, 35а', 'texnik': 'dmutro'},
    {'id_terem': 221, 'adress': 'В.Великого, 75', 'texnik': 'dmutro'},
    {'id_terem': 18, 'adress': 'В.Великого, 103', 'texnik': 'dmutro'},
    {'id_terem': 234, 'adress': 'Залізнична, 21', 'texnik': 'dmutro'},
    {'id_terem': 209, 'adress': 'Золота, 25', 'texnik': 'dmutro'},
    {'id_terem': 224, 'adress': 'Кн.Ольги, 98л', 'texnik': 'dmutro'},
    {'id_terem': 175, 'adress': 'Кн.Ольги, 100к', 'texnik': 'dmutro'},
    {'id_terem': 293, 'adress': 'Коновальця, 50', 'texnik': 'dmutro'},
    {'id_terem': 197, 'adress': 'Кропивницького, 7/9', 'texnik': 'dmutro'},
    {'id_terem': 187, 'adress': 'Кульпарківська 93', 'texnik': 'dmutro'},
    {'id_terem': 213, 'adress': 'Кульпарківська, 145', 'texnik': 'dmutro'},
    {'id_terem': 306, 'adress': 'Кульпарківська, 172', 'texnik': 'dmutro'},
    {'id_terem': 294, 'adress': 'Кульпарківська, 59', 'texnik': 'dmutro'},
    {'id_terem': 337, 'adress': 'Любінська, 4', 'texnik': 'dmutro'},
    {'id_terem': 287, 'adress': 'Марка Вовчка, 24', 'texnik': 'dmutro'},
    {'id_terem': 199, 'adress': 'Мундяк Марії, 8', 'texnik': 'dmutro'},
    {'id_terem': 229, 'adress': 'Наукова, 59', 'texnik': 'dmutro'},
    {'id_terem': 245, 'adress': 'Наукова, 96', 'texnik': 'dmutro'},
    {'id_terem': 343, 'adress': 'Наукова 10', 'texnik': 'dmutro'},
    {'id_terem': 182, 'adress': 'Повітряна, 78', 'texnik': 'dmutro'},
    {'id_terem': 276, 'adress': 'Рудненська, 8ж', 'texnik': 'dmutro'},
    {'id_terem': 321, 'adress': 'Федьковича, 24', 'texnik': 'dmutro'},
    {'id_terem': 176, 'adress': 'Федьковича, 38', 'texnik': 'dmutro'},
    {'id_terem': 256, 'adress': 'Художня, 4', 'texnik': 'dmutro'},
    {'id_terem': 317, 'adress': 'Цегельского, 10', 'texnik': 'dmutro'},
    {'id_terem': 278, 'adress': 'Чупринки, 84', 'texnik': 'dmutro'},
    {'id_terem': 247, 'adress': 'Шевченка, 31б', 'texnik': 'dmutro'},
    {'id_terem': 189, 'adress': 'Шевченка, 45', 'texnik': 'dmutro'},
    {'id_terem': 177, 'adress': 'Шевченка, 80', 'texnik': 'dmutro'},
    {'id_terem': 210, 'adress': 'Широка, 96а', 'texnik': 'dmutro'},
    {'id_terem': 259, 'adress': 'Васильківського, 9', 'texnik': 'dmutro'},
    {'id_terem': 275, 'adress': 'Героїв УПА, 73в', 'texnik': 'dmutro'},
    {'id_terem': 253, 'adress': 'Золота, 30', 'texnik': 'dmutro'},
    {'id_terem': 260, 'adress': 'Юнаківа, 9б', 'texnik': 'dmutro'},
    {'id_terem': 214, 'adress': 'Суботівська, 7', 'texnik': 'dmutro'},
    {'id_terem': 323, 'adress': 'Суботівська, 10а', 'texnik': 'dmutro'},
    {'id_terem': 204, 'adress': 'Роксоляни, 57', 'texnik': 'dmutro'},
    {'id_terem': 301, 'adress': 'Коперніка, 56', 'texnik': 'dmutro'},
    {'id_terem': 241, 'adress': 'Дзиндри, 1а', 'texnik': 'dmutro'},
    {'id_terem': 121, 'adress': 'Сахарова, 60', 'texnik': 'dmutro'},
    {'id_terem': 228, 'adress': 'Сокільники, Весняна, 18', 'texnik': 'dmutro'},
    {'id_terem': 341, 'adress': 'Сокільники, Збройних сил України, 2', 'texnik': 'dmutro'},
    {'id_terem': 302, 'adress': 'Сокільники, Г.Сковороди, 56', 'texnik': 'dmutro'},
    {'id_terem': 120, 'adress': 'Мікльоша, 17', 'texnik': 'dmutro'},
    {'id_terem': 340, 'adress': 'Гашека, 17', 'texnik': 'dmutro'},
    {'id_terem': 50, 'adress': 'Стрийська, 61', 'texnik': 'dmutro'},
    {'id_terem': 265, 'adress': 'Стрийська, 115', 'texnik': 'dmutro'},
    {'id_terem': 344, 'adress': 'Брюховичі, Весняна, 1а', 'texnik': 'ruslan'},
    {'id_terem': 235, 'adress': 'Лисеницька, 9', 'texnik': 'igor'},
]

# --- ФУНКЦИЯ УМНОГО ПОИСКА ---
def search_terem_info(user_input_address):
    """
    Шукає у списку ADDRESS_DB максимально схожа адреса.
    Повертає словник (id_terem, adress, texnik) або None.
    """
    if not user_input_address:
        return None

    # Создаем список только из адресов для поиска
    all_addresses = [item['adress'] for item in ADDRESS_DB]
    
    # Ищем лучшее совпадение
    # extractOne вернет кортеж: ('Найденная строка', score)
    best_match, score = process.extractOne(user_input_address, all_addresses, scorer=fuzz.token_set_ratio)
    
    # Лог для отладки
    logging.info(f"🔍 Пошук: '{user_input_address}' -> Знайдено: '{best_match}' (Точність: {score}%)")

    # Если точность выше 60%, считаем что нашли
    if score > 60:
        # Знаходимо повний об'єкт у базі
        for item in ADDRESS_DB:
            if item['adress'] == best_match:
                return item
    
    return None

# --- РАБОТА С БД ---
def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def save_stol_zakazov(user_role, text):
    conn = sqlite3.connect('my_database.db') # или как у вас называется файл
    cursor = conn.cursor()
    # ... ваш SQL запрос INSERT ...
    conn.commit()  # <--- БЕЗ ЭТОГО ДАННЫЕ НЕ СОХРАНЯТСЯ
    conn.close()

def init_tables():
    """Створює всі необхідні таблиці"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. Таблиця ОБИЧНЫХ ЗАДАЧ (zadaci_all)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zadaci_all (
                id SERIAL PRIMARY KEY,
                id_terem VARCHAR(50),
                adres TEXT,
                zadaca TEXT,
                texnik VARCHAR(50),
                date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'open',
                date_time_closed TIMESTAMP,
                day_time_vupolnyalos INTEGER
            );
        """)
        
        # 2. Таблиця КАРТ (kartu_all)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kartu_all (
                id SERIAL PRIMARY KEY,
                id_terem VARCHAR(50),
                adres TEXT,
                kartu TEXT,
                texnik VARCHAR(50),
                date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'open',
                date_time_closed TIMESTAMP,
                day_time_vupolnyalos INTEGER
            );
        """)

        # 3. Таблиця СРОЧНО (srochno_callcentr)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS srochno_callcentr (
                id SERIAL PRIMARY KEY,
                id_terem VARCHAR(50),
                adres TEXT,
                srocno TEXT,
                texnik VARCHAR(50),
                date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'open',
                date_time_closed TIMESTAMP,
                day_time_vupolnyalos INTEGER
            );
        """)

        # 4. Таблиця ЗАВДАНЬ З ТЕРМІНОМ (zavdanya_termin) - ВИПРАВЛЕНО
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zavdanya_termin (
                id SERIAL PRIMARY KEY,
                id_terem VARCHAR(50),
                adres TEXT,
                zavdanya TEXT,
                termin INTEGER,
                texnik VARCHAR(50),
                date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'open',
                date_time_closed TIMESTAMP,
                day_time_vupolnyalos INTEGER
            );
        """)
   
        conn.commit()
        conn.close()
        logging.info("✅ Таблиці zadaci_all, kartu_all, srochno_callcentr, zavdanya_termin перевірені.")
    except Exception as e:
        logging.error(f"❌ Помилка ініціалізації БД: {e}")

# --- ФУНКЦИИ СОХРАНЕНИЯ ---
def save_zadaca(id_terem, adres, zadaca, texnik):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO zadaci_all (id_terem, adres, zadaca, texnik) VALUES (%s, %s, %s, %s)",
        (id_terem, adres, zadaca, texnik)
    )
    conn.commit()
    conn.close()

def save_kartu(id_terem, adres, kartu, texnik):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO kartu_all (id_terem, adres, kartu, texnik) VALUES (%s, %s, %s, %s)",
        (id_terem, adres, kartu, texnik)
    )
    conn.commit()
    conn.close()

def save_termin_task(id_terem, adres, zavdanya, termin, texnik):
    """Зберігає завдання з терміном в таблицю zavdanya_termin"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO zavdanya_termin (id_terem, adres, zavdanya, termin, texnik) 
               VALUES (%s, %s, %s, %s, %s)""",
            (id_terem, adres, zavdanya, termin, texnik)
        )
        conn.commit()
        conn.close()
        logging.info(f"✅ Завдання з терміном збережено: {zavdanya} для {texnik}")
        return True
    except Exception as e:
        logging.error(f"❌ Помилка збереження завдання з терміном: {e}")
        return False

def get_termin_tasks(texnik_name):
    """Отримує відкриті завдання з терміном для конкретного техніка"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT id, adres, zavdanya, date_time_open, termin
            FROM zavdanya_termin 
            WHERE texnik = %s AND status = 'open'
            ORDER BY date_time_open DESC
        """
        cur.execute(query, (texnik_name,))
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"❌ Помилка отримання завдань з терміном: {e}")
        return []

def save_srochno(id_terem, adres, srocno, texnik):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO srochno_callcentr (id_terem, adres, zavdanya, termin, texnik) VALUES (%s, %s, %s, %s, %s, %s)",
        (id_terem, adres, zavdanya, termin, texnik)
    )
    conn.commit()
    conn.close()

def save_srochno(id_terem, adres, srocno, texnik):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO srochno_callcentr (id_terem, adres, srocno, texnik) VALUES (%s, %s, %s, %s)",
        (id_terem, adres, srocno, texnik)
    )
    conn.commit()
    conn.close()

def init_shared_tables():
    """Создает ОБЩИЕ таблицы для Стола заказов и Затрат (используются всеми техниками)"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 4. Стол заказов (stol_zakazov) - ОБЩАЯ таблица
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stol_zakazov (
                id SERIAL PRIMARY KEY,
                texnik VARCHAR(50),
                zakaz TEXT,
                date_time_open TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'open',
                date_time_closed TIMESTAMP
            );
        """)

        # 5. Затраты (zatratu_all) - ОБЩАЯ таблица
        cur.execute("""
            CREATE TABLE IF NOT EXISTS zatratu_all (
                id SERIAL PRIMARY KEY,
                texnik VARCHAR(50),
                zatrata TEXT,
                suma_zatrat NUMERIC, 
                status VARCHAR(20) DEFAULT 'open'
            );
        """)
        
        conn.commit()
        conn.close()
        logging.info("✅ Общие таблицы (stol_zakazov, zatratu_all) проверены.")
    except Exception as e:
        logging.error(f"❌ Ошибка init_shared_tables: {e}")

# Алиасы для обратной совместимости
def init_ruslan_tables():
    """Алиас для инициализации общих таблиц"""
    init_shared_tables()

def init_dmutro_tables():
    """Алиас для инициализации общих таблиц"""
    init_shared_tables()

def init_igor_tables():
    """Алиас для инициализации общих таблиц"""
    init_shared_tables()

# --- ФУНКЦИИ ДЛЯ РУСЛАНА ---
def get_ruslan_tasks(table_name):
    """Получает открытые задачи для техника ruslan из указанной таблицы"""
    conn = get_connection()
    cur = conn.cursor()
    
    desc_col = "zadaca"
    if table_name == "srochno_callcentr":
        desc_col = "srocno"
    elif table_name == "kartu_all":
        desc_col = "kartu"

    query = f"""
        SELECT id, adres, {desc_col}, date_time_open 
        FROM {table_name} 
        WHERE texnik = 'ruslan' AND status = 'open'
        ORDER BY date_time_open DESC
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

def close_task_in_db(table_name, task_id):
    """
    Закрывает задачу:
    1. Ставит status = 'closed'
    2. Ставит date_time_closed = NOW()
    3. Считает разницу в минутах и пишет в day_time_vupolnyalos
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Хитрая SQL магия для подсчета времени сразу в базе
        cur.execute(f"""
            UPDATE {table_name}
            SET 
                status = 'closed',
                date_time_closed = CURRENT_TIMESTAMP,
                day_time_vupolnyalos = EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - date_time_open)) / 60
            WHERE id = %s
        """, (task_id,))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Ошибка закрытия задачи: {e}")
        return False

def save_stol_zakazov(texnik, zakaz):
    """Сохраняет заказ В ОБЩУЮ таблицу stol_zakazov с указанием техника"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO stol_zakazov (texnik, zakaz) VALUES (%s, %s)",
        (texnik, zakaz)
    )
    conn.commit()
    conn.close()
    logging.info(f"✅ Заказ от {texnik} сохранен в stol_zakazov")

def save_zatrata(texnik, zatrata_name, suma):
    """Сохраняет затрату В ОБЩУЮ таблицу zatratu_all с указанием техника"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO zatratu_all (texnik, zatrata, suma_zatrat) VALUES (%s, %s, %s)",
        (texnik, zatrata_name, suma)
    )
    conn.commit()
    conn.close()
    logging.info(f"✅ Затрата от {texnik} сохранена в zatratu_all")

# --- ФУНКЦИИ ДЛЯ ДМИТРА ---
def get_dmutro_tasks(table_name):
    """Получает открытые задачи для техника dmutro"""
    conn = get_connection()
    cur = conn.cursor()
    
    desc_col = "zadaca"
    if table_name == "srochno_callcentr":
        desc_col = "srocno"
    elif table_name == "kartu_all":
        desc_col = "kartu"

    query = f"""
        SELECT id, adres, {desc_col}, date_time_open 
        FROM {table_name} 
        WHERE texnik = 'dmutro' AND status = 'open'
        ORDER BY date_time_open DESC
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

# --- ФУНКЦИИ ДЛЯ Ігора ---
def get_igor_tasks(table_name):
    """Получает открытые задачи для техника igor (ИСПРАВЛЕНО: igor с маленькой буквы)"""
    conn = get_connection()
    cur = conn.cursor()
    
    desc_col = "zadaca"
    if table_name == "srochno_callcentr":
        desc_col = "srocno"
    elif table_name == "kartu_all":
        desc_col = "kartu"

    query = f"""
        SELECT id, adres, {desc_col}, date_time_open 
        FROM {table_name} 
        WHERE texnik = 'igor' AND status = 'open'
        ORDER BY date_time_open DESC
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

# --- ФУНКЦИИ ДЛЯ ФИНАНСИСТА ---
def get_all_zatratu(status):
    """
    Получает список затрат из zatratu_all по статусу.
    Сортирует: сначала старые (для обработки) или новые (для архива).
    """
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT id, texnik, zatrata, suma_zatrat, status 
        FROM zatratu_all 
        WHERE status = %s
        ORDER BY id ASC
    """
    cur.execute(query, (status,))
    rows = cur.fetchall()
    conn.close()
    return rows

def close_zatrata_status(zatrata_id):
    """Меняет статус затраты на 'closed'"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE zatratu_all SET status = 'closed' WHERE id = %s",
            (zatrata_id,)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Помилка закриття витрати: {e}")
        return False

# --- ФУНКЦИИ ДЛЯ SUPER ADMIN ---
def get_super_analytics_top15():
    """
    Топ 15 самых долгих задач за последние 30 дней.
    Объединяет таблицы srochno_callcentr и zadaci_all.
    """
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT source, id, id_terem, adres, texnik, day_time_vupolnyalos 
        FROM (
            SELECT 'Срочно' as source, id, id_terem, adres, texnik, day_time_vupolnyalos 
            FROM srochno_callcentr 
            WHERE date_time_open >= NOW() - INTERVAL '30 days' 
              AND status = 'closed'
            UNION ALL
            SELECT 'Задача' as source, id, id_terem, adres, texnik, day_time_vupolnyalos 
            FROM zadaci_all 
            WHERE date_time_open >= NOW() - INTERVAL '30 days'
              AND status = 'closed'
        ) as combined
        ORDER BY day_time_vupolnyalos DESC
        LIMIT 15;
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

def get_avg_time_by_texnik(texnik_name):
    """Считает среднее время выполнения (day_time_vupolnyalos) для техника"""
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT AVG(day_time_vupolnyalos) 
        FROM (
            SELECT day_time_vupolnyalos FROM srochno_callcentr 
            WHERE texnik = %s AND status = 'closed' AND date_time_open >= NOW() - INTERVAL '30 days'
            UNION ALL
            SELECT day_time_vupolnyalos FROM zadaci_all 
            WHERE texnik = %s AND status = 'closed' AND date_time_open >= NOW() - INTERVAL '30 days'
        ) as combined;
    """
    cur.execute(query, (texnik_name, texnik_name))
    result = cur.fetchone()
    conn.close()
    return result[0] if result and result[0] else 0.0

def get_recurring_issues():
    """
    Ищет аппараты, которые ломались 2 и более раз за 30 дней.
    """
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT id_terem, COUNT(*) as cnt, MAX(adres), MAX(texnik)
        FROM (
            SELECT id_terem, adres, texnik FROM srochno_callcentr 
            WHERE date_time_open >= NOW() - INTERVAL '30 days'
            UNION ALL
            SELECT id_terem, adres, texnik FROM zadaci_all 
            WHERE date_time_open >= NOW() - INTERVAL '30 days'
        ) as combined
        GROUP BY id_terem
        HAVING COUNT(*) >= 2
        ORDER BY cnt DESC;
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

# --- Работа со СТОЛОМ ЗАКАЗОВ для Админа ---
def get_all_stol_zakazov(status):
    """Получает заказы из stol_zakazov по статусу"""
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT id, texnik, zakaz, date_time_open 
        FROM stol_zakazov 
        WHERE status = %s 
        ORDER BY id ASC
    """
    cur.execute(query, (status,))
    rows = cur.fetchall()
    conn.close()
    return rows

def close_stol_zakaz(order_id):
    """Закрывает заказ в столе заказов"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "UPDATE stol_zakazov SET status = 'closed', date_time_closed = CURRENT_TIMESTAMP WHERE id = %s",
            (order_id,)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Помилка закриття замовлення: {e}")
        return False

def get_all_open_tasks(table_name):
    """
    Получает ВСЕ открытые задачи из указанной таблицы, 
    независимо от техника.
    Возвращает: id, adres, описание, date_time_open, texnik.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    desc_col = "zadaca"
    if table_name == "srochno_callcentr":
        desc_col = "srocno"
    elif table_name == "kartu_all":
        desc_col = "kartu"

    query = f"""
        SELECT id, adres, {desc_col}, date_time_open, texnik
        FROM {table_name} 
        WHERE status = 'open'
        ORDER BY date_time_open DESC
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

# --- ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ ОТЧЕТОВ ИЗ БД ---
def get_latest_report_from_db(filename):
    """
    Получает последний отчет из таблицы automation_txt_files по имени файла.
    Возвращает содержимое отчета или None.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT content
            FROM automation_txt_files
            WHERE filename = %s
            ORDER BY created_at DESC
            LIMIT 1;
        """
        cur.execute(query, (filename,))
        row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if row:
            return row[0]
        else:
            return None
            
    except Exception as e:
        logging.error(f"❌ Ошибка получения отчета {filename} из БД: {e}")
        return None

def get_latest_ruslan_report():
    """Получает последний отчет Руслана из БД"""
    return get_latest_report_from_db("otchet_ruslan.txt")

def get_latest_dmutro_report():
    """Получает последний отчет Дмитра из БД"""
    return get_latest_report_from_db("otchet_dmutro.txt")

def get_latest_igor_report():
    """Получает последний отчет Игоря из БД"""
    return get_latest_report_from_db("otchet_igor.txt")

def get_latest_general_report():
    """Получает последний общий отчет из БД"""
    return get_latest_report_from_db("otchet_general.txt")

def get_latest_ink_report():
    """Получает последний отчет инкассаций из БД"""
    return get_latest_report_from_db("otchet_inki.txt")

def get_latest_service_report():
    """Получает последний service отчет из БД"""
    return get_latest_report_from_db("service_glub_analitik.txt")

# Додайте ці функції в кінець файлу database.py

def get_inki_week_data():
    """
    Отримує дані інкасацій за останні 7 днів з таблиці inki5nedel
    Повертає список кортежів: (device_id, address, date, banknotes, coins, tech)
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT device_id, address, date, banknotes, coins, tech
            FROM inki5nedel
            WHERE date::date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY tech ASC, date ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        
        logging.info(f"✅ Отримано {len(rows)} записів інкасацій за 7 днів")
        return rows
        
    except Exception as e:
        logging.error(f"❌ Помилка отримання даних інкасацій: {e}")
        return []


def generate_inki_week_report_file():
    """
    Генерує файл звіту по інкасаціям за тиждень
    Повертає шлях до файлу або None у разі помилки
    """
    file_path = "inki_nedelya.txt"
    
    try:
        rows = get_inki_week_data()
        
        if not rows:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("📂 За останні 7 днів інкасацій не знайдено.\n")
            return file_path

        # Списки для зберігання даних
        suspicious_lines = []
        report_lines = []

        # Заголовок звіту
        report_lines.append("📅 ЗВІТ ПО ІНКАСАЦІЯМ (ОСТАННІ 7 ДНІВ)\n")
        report_lines.append(f"{'ТЕХНІК':<15} | {'ДАТА':<10} | {'БАНКНОТИ':<10} | {'МОНЕТИ':<8} | АДРЕСА")
        report_lines.append("-" * 80)

        # Обробка даних
        for row in rows:
            dev_id, addr, date_obj, banknotes, coins, tech = row
            
            # Перетворюємо суми в числа
            b_sum = float(banknotes) if banknotes else 0.0
            c_sum = float(coins) if coins else 0.0
            date_str = str(date_obj)

            # Додаємо рядок в основний звіт
            line = f"{tech:<15} | {date_str[:10]:<10} | {int(b_sum):<10} | {int(c_sum):<8} | {addr} (ID:{dev_id})"
            report_lines.append(line)

            # Перевіряємо на підозрілі суми (> 15000)
            if b_sum > 15000 or c_sum > 15000:
                susp_line = f"ID: {dev_id} | {addr} | 💵: {int(b_sum)} | 🪙: {int(c_sum)} | 👤 {tech} | 📅 {date_str}"
                suspicious_lines.append(susp_line)

        # Записуємо у файл
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))
            f.write("\n\n")
            
            # Додаємо секцію з підозрілими інкасаціями
            if suspicious_lines:
                f.write("_" * 78 + "\n")
                f.write("💹💲️Подозрительные инкасации💲️💹\n")
                f.write("_" * 78 + "\n\n")
                f.write("\n".join(suspicious_lines))
            else:
                f.write("✅ Підозрілих інкасацій (понад 15000) не виявлено.\n")

        logging.info(f"✅ Звіт збережено: {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"❌ Помилка створення звіту: {e}")
        return None
        
def get_inki_5week_data():
    """
    Возвращает данные инкасаций за последние 35 дней (5 недель)
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        query = """
            SELECT device_id, address, date, banknotes, coins, tech
            FROM inki5nedel
            WHERE date::date >= CURRENT_DATE - INTERVAL '35 days'
            ORDER BY tech ASC, date ASC;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        cur.close()
        conn.close()

        return rows

    except Exception as e:
        logging.error(f"Ошибка получения 5-недельного отчёта: {e}")
        return []
        
def generate_inki_5week_file():
    """
    Создаёт файл инкасаций за 5 недель.
    Возвращает путь к файлу.
    """
    file_path = "inki_5week.txt"
    rows = get_inki_5week_data()

    try:
        text_lines = []
        suspicious = []

        text_lines.append("💰 ЗВІТ ПО ІНКАСАЦІЯМ (ОСТАННІ 5 ТИЖНІВ)\n")
        text_lines.append(f"{'ТЕХНІК':<15} | {'ДАТА':<10} | {'БАНКНОТИ':<10} | {'МОНЕТИ':<10} | АДРЕСА")
        text_lines.append("-" * 85)

        for d_id, addr, date, b, c, tech in rows:
            # --- ЗАЩИТА ОТ NONE ---
            tech = tech or "—"
            addr = addr or "—"
            d_id = d_id or "—"

            # --- ДАТА ---
            if date is None:
                date_str = "—"
            else:
                date_str = str(date)[:10]

            # --- СУММЫ ---
            try:
                b_val = int(float(b)) if b not in (None, "", " ") else 0
            except:
                b_val = 0

            try:
                c_val = int(float(c)) if c not in (None, "", " ") else 0
            except:
                c_val = 0

            # Основная строка отчёта
            text_lines.append(
                f"{tech:<15} | {date_str:<10} | {b_val:<10} | {c_val:<10} | {addr} (ID:{d_id})"
            )

            # Подозрительные суммы
            if b_val > 15000 or c_val > 15000:
                suspicious.append(
                    f"ID:{d_id} | {addr} | 💵{b_val} | 🪙{c_val} | 👤 {tech} | 📅 {date_str}"
                )

        text_lines.append("\n")

        if suspicious:
            text_lines.append("_" * 80)
            text_lines.append("💹 ПІДОЗРІЛІ ІНКАСАЦІЇ")
            text_lines.append("_" * 80)
            text_lines.extend(suspicious)
        else:
            text_lines.append("✅ Підозрілих інкасацій не виявлено.")

        # Запис файла
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(text_lines))

        return file_path

    except Exception as e:
        logging.error(f"Ошибка создания файла отчёта: {e}")
        return None