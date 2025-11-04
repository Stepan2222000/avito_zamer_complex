"""
Модуль валидации объявлений Авито

Экспортирует функции механической и ИИ-валидации
"""

from .mechanical import validate_mechanical
from .ai import validate_ai

__all__ = ['validate_mechanical', 'validate_ai']
