import mysql.connector
from abc import ABC, abstractmethod

class DatabaseConnection(ABC):
    
    @abstractmethod
    def get_connection(self):
        pass

class MySQLConnection(DatabaseConnection):
    """Реализация подключения к MySQL"""
    
    def __init__(self, host, user, password, database):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
    
    def get_connection(self):
        return mysql.connector.connect(**self.config)