import argparse
import os
from models.database import MySQLConnection
from services.data_loader import JSONDataLoader, DatabaseManager
from services.query_service import QueryService
from exporters.data_exporter import ExportManager
from config import DB_CONFIG

class StudentRoomAnalyzer:
    
    def __init__(self, db_config):
        self.db_connection = MySQLConnection(**db_config)
        self.data_loader = JSONDataLoader()
        self.db_manager = DatabaseManager(self.db_connection)
        self.query_service = QueryService(self.db_connection)
        self.export_manager = ExportManager()
    
    def run(self, students_file, rooms_file, output_format, output_dir=None):
        """Основной метод запуска приложения"""
        
        print("🚀 Starting Student Room Analysis...")
        
        # Создаем папку для результатов если её нет
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"📁 Created output directory: {output_dir}")
        
        # 1. Инициализация базы данных
        print("📊 Initializing database...")
        self.db_manager.initialize_database()
        
        # 2. Загрузка данных
        print("📁 Loading data...")
        rooms_data = self.data_loader.load_data(rooms_file)
        students_data = self.data_loader.load_data(students_file)
        
        print(f"   Loaded {len(rooms_data)} rooms and {len(students_data)} students")
        
        # 3. Очистка и загрузка новых данных
        self.db_manager.clear_tables()
        self.db_manager.load_rooms(rooms_data)
        self.db_manager.load_students(students_data)
        print("✅ Data loaded into database")
        
        # 4. Выполнение всех запросов и сохранение в отдельные файлы
        print("🔍 Executing queries and saving results...")
        
        # Словарь для соответствия имен запросов и названий файлов
        queries = {
            'rooms_student_count': 'rooms_with_student_count',
            'rooms_min_avg_age': 'rooms_with_min_avg_age', 
            'rooms_max_age_diff': 'rooms_with_max_age_diff',
            'rooms_mixed_gender': 'rooms_with_mixed_gender'
        }
        
        for query_key, file_name in queries.items():
            # Выполняем запрос
            if query_key == 'rooms_student_count':
                result = self.query_service.get_rooms_student_count()
            elif query_key == 'rooms_min_avg_age':
                result = self.query_service.get_rooms_min_avg_age()
            elif query_key == 'rooms_max_age_diff':
                result = self.query_service.get_rooms_max_age_diff()
            elif query_key == 'rooms_mixed_gender':
                result = self.query_service.get_rooms_mixed_gender()
            
            # Сохраняем в отдельный файл
            if output_dir:
                # Формируем путь к файлу
                output_file = os.path.join(output_dir, f"{file_name}.{output_format}")
                
                # Экспортируем результат этого запроса
                exporter = self.export_manager.get_exporter(output_format)
                exporter.export({query_key: result}, output_file)
                
                print(f"   ✅ {query_key} - saved to: {output_file}")
            else:
                # Если папка не указана - выводим на экран
                print(f"   ✅ {query_key} - DONE")
                exporter = self.export_manager.get_exporter(output_format)
                print(exporter.export({query_key: result}))
        
        # 5. Также сохраняем объединенные результаты в один файл (опционально)
        if output_dir:
            print(f"\n💾 Saving combined results...")
            combined_results = {}
            for query_key in queries.keys():
                if query_key == 'rooms_student_count':
                    combined_results[query_key] = self.query_service.get_rooms_student_count()
                elif query_key == 'rooms_min_avg_age':
                    combined_results[query_key] = self.query_service.get_rooms_min_avg_age()
                elif query_key == 'rooms_max_age_diff':
                    combined_results[query_key] = self.query_service.get_rooms_max_age_diff()
                elif query_key == 'rooms_mixed_gender':
                    combined_results[query_key] = self.query_service.get_rooms_mixed_gender()
            
            combined_file = os.path.join(output_dir, f"combined_results.{output_format}")
            exporter = self.export_manager.get_exporter(output_format)
            exporter.export(combined_results, combined_file)
            print(f"   ✅ Combined results saved to: {combined_file}")
        
        print("🎉 Analysis completed successfully!")

def main():
    parser = argparse.ArgumentParser(description='Student Room Analysis Tool')
    parser.add_argument('--students', required=True, help='Path to students JSON file')
    parser.add_argument('--rooms', required=True, help='Path to rooms JSON file')
    parser.add_argument('--format', choices=['json', 'xml'], required=True, help='Output format')
    parser.add_argument('--output-dir', help='Output directory for separate files (optional)')
    
    args = parser.parse_args()
    
    # Создаем и запускаем анализатор
    analyzer = StudentRoomAnalyzer(DB_CONFIG)
    analyzer.run(args.students, args.rooms, args.format, args.output_dir)

if __name__ == "__main__":
    main()