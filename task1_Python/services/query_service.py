from decimal import Decimal
import json

class QueryService:
    """Сервис для выполнения запросов к базе данных"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def execute_query(self, query, params=None):
        """Универсальный метод выполнения запросов с конвертацией Decimal"""
        conn = self.db_connection.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        try:
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            
            # Конвертируем Decimal в float для JSON сериализации
            return self._convert_decimals_to_float(result)
        finally:
            cursor.close()
            conn.close()
    
    def _convert_decimals_to_float(self, data):
        """Рекурсивно конвертирует Decimal в float"""
        if isinstance(data, list):
            return [self._convert_decimals_to_float(item) for item in data]
        elif isinstance(data, dict):
            return {key: self._convert_decimals_to_float(value) for key, value in data.items()}
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data
    
    def get_rooms_student_count(self):
        """Список комнат и количество студентов в каждой из них"""
        query = """
            SELECT rooms.id, rooms.name, COUNT(students.id) AS students_count
            FROM rooms LEFT JOIN students ON rooms.id = students.room_id
            GROUP BY rooms.id, rooms.name
            ORDER BY rooms.id
        """
        return self.execute_query(query)
    
    def get_rooms_min_avg_age(self, limit=5):
        """5 комнат с самым маленьким средним возрастом студентов"""
        query = """
            SELECT rooms.id, rooms.name, 
                   AVG(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) AS avg_age
            FROM rooms JOIN students ON students.room_id = rooms.id
            GROUP BY rooms.id, rooms.name
            ORDER BY avg_age ASC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_rooms_max_age_diff(self, limit=5):
        """5 комнат с самой большой разницей в возрасте студентов"""
        query = """
            SELECT rooms.id, rooms.name, 
                   (MAX(TIMESTAMPDIFF(YEAR, birthday, CURDATE())) - 
                    MIN(TIMESTAMPDIFF(YEAR, birthday, CURDATE()))) as age_diff
            FROM rooms JOIN students ON students.room_id = rooms.id
            GROUP BY rooms.id, rooms.name
            ORDER BY age_diff DESC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_rooms_mixed_gender(self):
        """Список комнат, где проживают студенты разного пола"""
        query = """
            SELECT rooms.id, rooms.name
            FROM rooms JOIN students ON students.room_id = rooms.id
            GROUP BY rooms.id, rooms.name
            HAVING COUNT(DISTINCT students.sex) > 1
            ORDER BY rooms.id
        """
        return self.execute_query(query)