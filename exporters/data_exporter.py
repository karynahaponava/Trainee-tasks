import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod

class DataExporter(ABC):
    """Абстрактный класс для экспорта данных"""
    
    @abstractmethod
    def export(self, data, output_file=None):
        pass

class JSONExporter(DataExporter):
    """Экспорт в JSON формат"""
    
    def export(self, data, output_file=None):
        result = json.dumps(data, ensure_ascii=False, indent=2)
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(result)
        return result

class XMLExporter(DataExporter):
    """Экспорт в XML формат"""
    
    def export(self, data, output_file=None):
        root = ET.Element('student_analysis')
        
        # Добавляем результаты каждого запроса
        for query_name, query_data in data.items():
            query_element = ET.SubElement(root, query_name)
            
            if isinstance(query_data, list):
                for item in query_data:
                    self._add_dict_to_xml(item, query_element, 'item')
            else:
                self._add_dict_to_xml(query_data, query_element, 'result')
        
        # Форматирование
        self._indent(root)
        
        result = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
        
        if output_file:
            tree = ET.ElementTree(root)
            tree.write(output_file, encoding='utf-8', xml_declaration=True)
        
        return result
    
    def _add_dict_to_xml(self, data, parent, element_name):
        """Рекурсивно добавляет словарь в XML"""
        element = ET.SubElement(parent, element_name)
        for key, value in data.items():
            child = ET.SubElement(element, key)
            if isinstance(value, (dict, list)):
                if isinstance(value, list):
                    for item in value:
                        self._add_dict_to_xml(item if isinstance(item, dict) else {'value': item}, child, 'item')
                else:
                    self._add_dict_to_xml(value, child, 'detail')
            else:
                child.text = str(value)
        return element
    
    def _indent(self, elem, level=0):
        """Добавляет отступы для XML"""
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                self._indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

class ExportManager:
    """Менеджер для управления экспортерами"""
    
    def __init__(self):
        self.exporters = {
            'json': JSONExporter(),
            'xml': XMLExporter()
        }
    
    def get_exporter(self, format_type):
        if format_type not in self.exporters:
            raise ValueError(f"Unsupported format: {format_type}")
        return self.exporters[format_type]