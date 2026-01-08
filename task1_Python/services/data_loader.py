import json
from abc import ABC, abstractmethod

class DataLoader(ABC):
    
    @abstractmethod
    def load_data(self, file_path):
        pass

class JSONDataLoader(DataLoader):
    """Загрузка в JSON файлов"""
    def load_data(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

class DatabaseManager:
    """Класс для управления базой данных"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def initialize_database(self):
        """Инициализация базы данных и создание индексов"""
        conn = self.db_connection.get_connection()
        cursor = conn.cursor()
        
        # Создание таблиц (если их нет)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS students (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                birthday DATE NOT NULL,
                sex ENUM('M', 'F') NOT NULL,
                room_id INT,
                FOREIGN KEY (room_id) REFERENCES rooms(id)
            )
        """)
        
        # Создание индексов для оптимизации 
        indexes = [
            "CREATE INDEX idx_students_room_id ON students(room_id)",
            "CREATE INDEX idx_students_birthday ON students(birthday)", 
            "CREATE INDEX idx_students_sex ON students(sex)"
        ]
        
        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
                print(f"✅ Index created: {index_sql}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"ℹ️ Index already exists: {index_sql.split()[2]}")
                else:
                    print(f"⚠️ Could not create index: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Database initialized with indexes")
    
    def clear_tables(self):
        """Очистка таблиц перед загрузкой новых данных"""
        conn = self.db_connection.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM students")
        cursor.execute("DELETE FROM rooms")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Tables cleared")
    
    def load_rooms(self, rooms_data):
        """Загрузка данных о комнатах"""
        conn = self.db_connection.get_connection()
        cursor = conn.cursor()
        
        for room in rooms_data:
            cursor.execute(
                "INSERT IGNORE INTO rooms (id, name) VALUES (%s, %s)",
                (room["id"], room["name"])
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Rooms data loaded")
    
    def load_students(self, students_data):
        """Загрузка данных о студентах"""
        conn = self.db_connection.get_connection()
        cursor = conn.cursor()
        
        for student in students_data:
            cursor.execute(
                "INSERT IGNORE INTO students (id, name, birthday, sex, room_id) VALUES (%s, %s, %s, %s, %s)",
                (
                    student["id"],
                    student["name"],
                    student["birthday"],
                    student["sex"],
                    student["room"]
                )
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Students data loaded")