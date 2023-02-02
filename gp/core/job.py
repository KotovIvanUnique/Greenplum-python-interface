# class Job():
#     '''
#     Класс для создания и редактирования Job'ов

#     Параметры
#     ----------
#     job_id: int, по умолчанию None; ID Job'а

#     Атрибуты
#     ----------
#     job_id: int, по умолчанию None; ID Job'а

#     Методы
#     ----------
#     create: метод для создания Job'а
#     refresh: метод для обновления Job'а
#     delete: метод для удаления Job'а
#     select: метод для вывода Job'а на печать
#     select_df: метод для вывода Job'а в датафрейм

#     Таблицы
#     ----------
#     '''

#     def __init__(self, job_id = None):
#         self.job_id = job_id

#     def create(self, process, schedule = '7d'):
#         '''
#         Метод для создания Job'а

#         Параметры
#         ----------
#         process: postgresql-код processа, запускаемого Job'ом
#         schedule: расписание Job'а, указывается в формате 1h - каждый час/1d - каждый день/7d - каждые семь дней/1m - каждый месяц
#         '''

#         pass

#     def refresh(self):
#         '''
#         Метод для обновления Job'а
#         '''

#         pass

#     def delete(self):
#         '''
#         Метод для удаления Job'а
#         '''

#         pass

#     def select(self):
#         '''
#         Метод для вывода Job'а на печать
#         '''

#         pass

#     def select_df(self):
#         '''
#         Метод для вывода Job'а в датафрейм
#         '''

#         pass