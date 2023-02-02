import psycopg2
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
from PIL import ImageColor
# from matplotlib_venn import venn2, venn3
# from plotly.subplots import make_subplots

class Stop(object):
    '''
    Класс для создания, изменения и обновления стоп-факторов

    Параметры
    ----------
    object: экземпляр класса cjm.GPConnector() для подключения к БД
    stop_id: int, по умолчанию None; ID стопа
    stop_cd: str, по умолчанию None; уникальный краткий код стопа
    Создание экземпляра класса Stop возможно как через stop_id, так и через stop_cd

    Атрибуты
    ----------
    gpconnector: экземпляр класса cjm.GPConnector() для подключения к БД
    stop_id: int, по умолчанию None; ID стопа
    stop_cd: str, по умолчанию None; уникальный краткий код стопа
    description: описание, отражающее смысл стопа; ограничение - 500 символов
    stop_type: {'MATERIALIZED VIEW', 'VIEW', 'TABLE', 'FUNCTION'}, по умолчанию 'MATERIALIZED VIEW'
    тип стопа, указывает создается ли стоп как материализованное представление (MATERIALIZED VIEW) или обычное представление
    schedule: расписание обновления стопа с типом MATERIALIZED VIEW, указывается в формате 1h - каждый час/1d - каждый день/7d - каждые семь дней/1m - каждый месяц
    create_dt: date; дата создания стопа
    refresh_dt: date; дата обновления стопа
    stop_dict: str; название таблицы со списком стопов
    stop_repository str; название таблицы с репозиторием стопов
    stop_template: str; шаблон названия стопа

    Методы
    ----------
    create: метод для создания стопа
    change: метод для изменения стопа
    refresh: метод для обновления стопа
    delete: метод для удаления стопа
    get_source_code: метод для вывода исходного кода стопа на печать
    get_description: метод для вывода описания стопа на печать
    select: метод для вывода стопа на печать
    select_list: метод для вывода стопа в list
    select_df: метод для вывода стопа в датафрейм
    load: метод для загрузки стопа типа 'MATERIALIZED VIEW' или 'TABLE' в репозиторий стопов

    Таблицы
    ----------
    prom.stop_repository: репозиторий стопов
    prom.stop_dict: справочник стопов
    prom.stop_{stop_id}: MATERIALIZED VIEW/VIEW/TABLE/FUNCTION стопа по данному stop_id
    '''

    def __init__(self, gpconnector, stop_id: int = None, stop_cd: str = None):
        self.gpconnector = gpconnector
        self.stop_dict = "prom.stop_dict"
        self.stop_template = "prom.stop_"
        self.stop_repository = "prom.stop_repository"
        self.stop_id = stop_id
        self.stop_cd = stop_cd

        if not stop_id and stop_cd:
            self.stop_id, self.description, self.stop_type, self.schedule, self.create_dt, self.refresh_dt = \
            gpconnector.select_list(
                """select stop_id, description, stop_type, schedule, create_dt, refresh_dt from {stop_dict} 
                where stop_cd = '{stop_cd}'""".format(stop_dict = self.stop_dict, stop_cd = stop_cd))[0]
        if not stop_cd and stop_id:
            self.stop_cd, self.description, self.stop_type, self.schedule, self.create_dt, self.refresh_dt = \
            gpconnector.select_list(
                """select stop_cd, description, stop_type, schedule, create_dt, refresh_dt from {stop_dict}
                where stop_id = {stop_id}""".format(stop_dict = self.stop_dict, stop_id = stop_id))[0]

    def create(self, target, description: str, stop_cd: str = None, stop_type: str = 'MATERIALIZED VIEW', schedule: str = '7d'):
        '''
        Метод для создания стопа

        Параметры
        ----------
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для создания стопа
        description: str; описание, отражающее смысл стопа; ограничение - 500 символов
        stop_cd: str, по умолчанию None; уникальный краткий код стопа
        stop_type: {MATERIALIZED VIEW', 'VIEW', 'TABLE', 'FUNCTION'}, по умолчанию 'MATERIALIZED VIEW'
        тип стопа, указывает создается ли стоп как материализованное представление (MATERIALIZED VIEW) или обычное представление
        schedule: расписание обновления стопа с типом MATERIALIZED VIEW, указывается в формате 1h - каждый час/1d - каждый день/7d - каждые семь дней/1m - каждый месяц
        '''

        if self.stop_id:
            raise Exception("ERROR: Стоп-фактор с таким stop_id уже существует!")
        elif self.gpconnector.select_list("select stop_id from {stop_dict} where stop_cd = '{stop_cd}'"
                                          .format(stop_dict = self.stop_dict, stop_cd = stop_cd)):
            raise Exception("ERROR: Стоп-фактор с таким stop_cd уже существует!")
        elif stop_type.upper() == 'FUNCTION' and 'create or replace function {function_name}' not in target:
            raise Exception(
                "ERROR: Стоп-фактор с типом 'FUNCTION' должен начинаться с 'create or replace function {function_name}'!")
        elif not self.gpconnector.validate_target(target):
            raise Exception("ERROR: Некорректный таргет!")
        else:
            self.stop_cd = stop_cd or self.stop_cd
            self.stop_type = stop_type.upper()
            self.description = description
            self.schedule = schedule

            self.gpconnector.insert(self.stop_dict
                                    , [(self.stop_cd, self.description, self.stop_type, self.schedule)]
                                    , columns = 'stop_cd, description, stop_type, schedule')
            self.stop_id = self.gpconnector.select_list("select last_value from {stop_dict}_stop_id_seq"
                                                        .format(stop_dict=self.stop_dict))[0][0]
            try:
                self.gpconnector.drop("{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))
                self.gpconnector.create(stop_type, "{stop_template}{stop_id}"
                                        .format(stop_template=self.stop_template, stop_id=self.stop_id), target)
                print("""SUCCESS: Стоп №{stop_id} "{stop_cd}" создан как {stop_type} {stop_template}{stop_id}"""
                      .format(stop_template=self.stop_template
                              , stop_id=self.stop_id
                              , stop_cd=self.stop_cd
                              , stop_type=self.stop_type))
            except psycopg2.Error as error:
                self.gpconnector.execute("delete from {stop_dict} where stop_id = {stop_id}"
                                         .format(stop_dict=self.stop_dict, stop_id=self.stop_id))
                print("ERROR: Ошибка при создании стопа: {error}".format(error=error))
                raise

    def change(self, target=None, stop_cd: str = None, description: str = None, stop_type: str = None,
               schedule: str = None, refresh_dt:datetime = None):
        '''
        Метод для изменения стопа

        Параметры
        ----------
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для создания стопа
        stop_cd: str, по умолчанию None; уникальный краткий код стопа
        description: str, по умолчанию None; краткое описание, отражающее смысл стопа; ограничение - 500 символов
        stop_type: {'MATERIALIZED VIEW', 'VIEW', 'TABLE', 'FUNCTION'}, по умолчанию None;
        тип стопа, указывает создается ли стоп как материализованное представление (MATERIALIZED VIEW) или обычное представление
        schedule: str, по умолчанию None; расписание обновления стопа с типом MATERIALIZED VIEW, указывается в формате 1h - каждый час/1d - каждый день/7d - каждые семь дней/1m - каждый месяц
        refresh_dt: datetime.datetime, по умолчанию None; дата последнего обновления стопа
        '''

        if target:
            if self.gpconnector.validate_target(target):
                self.gpconnector.drop(
                    "{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))
                self.gpconnector.create(stop_type or self.stop_type, "{stop_template}{stop_id}"
                                        .format(stop_template=self.stop_template, stop_id=self.stop_id), target)
            else:
                raise Exception("ERROR: Некорректный таргет!")

        if stop_type:
            stop_type = stop_type.upper()

            def update_stop_type(self, stop_cd, stop_type):
                self.gpconnector.execute("update {stop_dict} set stop_type = '{stop_type}' where stop_id = {stop_id}"
                                         .format(stop_dict=self.stop_dict
                                                 , stop_id=self.stop_id
                                                 , stop_type=stop_type))
                self.stop_type = stop_type
                print('SUCCESS: Стоп №{stop_id} "{stop_cd}" изменен как {stop_type} {stop_template}{stop_id}'
                      .format(stop_template=self.stop_template
                              , stop_id=self.stop_id
                              , stop_cd=stop_cd or self.stop_cd
                              , stop_type=stop_type))

            if not target:
                if stop_type == 'FUNCTION' or self.stop_type == 'FUNCTION':
                    raise Exception(
                        "ERROR: Для изменения стопа с типа 'FUNCTION'/на тип 'FUNCTION' необходимо передать sql-код функции в параметр target")
                elif stop_type != self.stop_type:
                    if self.stop_type == 'TABLE':
                        self.gpconnector.drop("{stop_template}{stop_id}_src".format(stop_template=self.stop_template,
                                                                                    stop_id=self.stop_id))
                        self.gpconnector.create('table',
                                                "{stop_template}{stop_id}_src".format(stop_template=self.stop_template,
                                                                                      stop_id=self.stop_id)
                                                , "select * from {stop_template}{stop_id}".format(
                                stop_template=self.stop_template, stop_id=self.stop_id))
                        self.gpconnector.drop(
                            "{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))
                        self.gpconnector.create(stop_type,
                                                "{stop_template}{stop_id}".format(stop_template=self.stop_template,
                                                                                  stop_id=self.stop_id)
                                                , "select * from {stop_template}{stop_id}_src".format(
                                stop_template=self.stop_template, stop_id=self.stop_id))
                        update_stop_type(self, stop_cd, stop_type)
                    else:
                        target = self.gpconnector.get_source_code(
                            '{stop_template}{stop_id}'.format(stop_template=self.stop_template, stop_id=self.stop_id))
                        if self.gpconnector.validate_target(target):
                            self.gpconnector.drop("{stop_template}{stop_id}".format(stop_template=self.stop_template,
                                                                                    stop_id=self.stop_id))
                            self.gpconnector.create(stop_type,
                                                    "{stop_template}{stop_id}".format(stop_template=self.stop_template,
                                                                                      stop_id=self.stop_id), target)
                            update_stop_type(self, stop_cd, stop_type)
                        else:
                            raise Exception("ERROR: Некорректный sql-код стопа!")
            else:
                update_stop_type(self, stop_cd, stop_type)

        if stop_cd:
            self.gpconnector.execute("update {stop_dict} set stop_cd = '{stop_cd}' where stop_id = {stop_id}"
                                     .format(stop_dict=self.stop_dict, stop_id=self.stop_id, stop_cd=stop_cd))
            self.stop_cd = stop_cd

        if description:
            self.gpconnector.execute("update {stop_dict} set description = '{description}' where stop_id = {stop_id}"
                                     .format(stop_dict=self.stop_dict, stop_id=self.stop_id, description=description))
            self.description = description

        if schedule:
            self.gpconnector.execute("update {stop_dict} set schedule = '{schedule}' where stop_id = {stop_id}"
                                     .format(stop_dict=self.stop_dict, stop_id=self.stop_id, schedule=schedule))
            self.schedule = schedule

        if refresh_dt:
            self.gpconnector.execute("update {stop_dict} set refresh_dt = cast('{refresh_dt}' as date) where stop_id = {stop_id}"
                                     .format(stop_dict=self.stop_dict, stop_id=self.stop_id, refresh_dt=refresh_dt))
            self.refresh_dt = self.gpconnector.select_list("select cast('{refresh_dt}' as date)".format(refresh_dt=refresh_dt))[0][0]

        print(self.gpconnector.select_df("""select * from {stop_dict} where stop_id = '{stop_id}'"""
                                         .format(stop_dict=self.stop_dict, stop_id=self.stop_id)).to_string(
            index=False))

    def refresh(self):
        '''
        Метод для обновления стопа
        '''

        self.gpconnector.refresh(
            "{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))
        self.change(refresh_dt=datetime.now())
        # self.gpconnector.execute("update {stop_dict} set refresh_dt = current_date where stop_id = {stop_id}"
        #                          .format(stop_dict=self.stop_dict, stop_id=self.stop_id))

    def delete(self):
        '''
        Метод для удаления стопа
        '''

        self.gpconnector.drop("{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))
        self.gpconnector.execute("""delete from {stop_repository} where stop_id = {stop_id};
                                    delete from {stop_dict} where stop_id = {stop_id};
                                 """.format(stop_repository=self.stop_repository, stop_dict=self.stop_dict,
                                            stop_id=self.stop_id))
        print('SUCCESS: Cтоп №{stop_id} "{stop_cd}" удален'.format(stop_id=self.stop_id, stop_cd=self.stop_cd))
        self.stop_id, self.stop_cd, self.description, self.stop_type, self.schedule, self.create_dt, self.refresh_dt = None, None, None, None, None, None, None

    def get_source_code(self) -> str:
        '''
        Метод для вывода исходного кода стопа на печать
        '''

        return self.gpconnector.get_source_code(
            "{stop_template}{stop_id}".format(stop_template=self.stop_template, stop_id=self.stop_id))

    def get_description(self):
        '''
        Метод для вывода описания стопа на печать
        '''

        if self.stop_id:
            self.gpconnector.select("""select * from {stop_dict} where stop_id = '{stop_id}'"""
                                    .format(stop_dict=self.stop_dict, stop_id=self.stop_id))
        else:
            self.gpconnector.select("""select * from {stop_dict}""".format(stop_dict=self.stop_dict))

    def select(self, limit: int = 100, arguments: str = None):
        '''
        Метод для вывода стопа на печать

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        arguments: str, по умолчанию None; аргументы для стопов с типом 'FUNCTION'
        '''

        if self.stop_type == 'FUNCTION':
            self.gpconnector.select("""select * from {stop_template}{stop_id}({arguments})"""
                                    .format(stop_template=self.stop_template
                                            , stop_id=self.stop_id
                                            , arguments=arguments), limit)
        else:
            self.gpconnector.select("""select * from {stop_template}{stop_id}"""
                                    .format(stop_template=self.stop_template
                                            , stop_id=self.stop_id), limit)

    def select_list(self, limit: int = 100, arguments: str = None) -> list:
        '''
        Метод для вывода стопа в list

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        arguments: str, по умолчанию None; аргументы для стопов с типом 'FUNCTION'
        '''
        if self.stop_type == 'FUNCTION':
            return self.gpconnector.select_list("""select * from {stop_template}{stop_id}({arguments})"""
                                                .format(stop_template=self.stop_template
                                                        , stop_id=self.stop_id
                                                        , arguments=arguments), limit)
        else:
            return self.gpconnector.select_list("""select * from {stop_template}{stop_id}"""
                                                .format(stop_template=self.stop_template
                                                        , stop_id=self.stop_id), limit)

    def select_df(self, limit: int = 100, arguments: str = None) -> pd.DataFrame:
        '''
        Метод для вывода стопа в датафрейм

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        arguments: str, по умолчанию None; аргументы для стопов с типом 'FUNCTION'
        '''
        if self.stop_type == 'FUNCTION':
            return self.gpconnector.select_df("""select * from {stop_template}{stop_id}({arguments})"""
                                              .format(stop_template=self.stop_template
                                                      , stop_id=self.stop_id
                                                      , arguments=arguments), limit)
        else:
            return self.gpconnector.select_df("""select * from {stop_template}{stop_id}"""
                                              .format(stop_template=self.stop_template
                                                      , stop_id=self.stop_id), limit)

    def load(self):
        '''
        Метод для загрузки стопа типа 'MATERIALIZED VIEW' или 'TABLE' в репозиторий стопов
        '''

        if self.stop_type in ['TABLE', 'MATERIALIZED VIEW']:
            self.gpconnector.insert(self.stop_repository
                                    , "select {stop_id} as stop_id, inn from {stop_template}{stop_id}"
                                    .format(stop_template=self.stop_template
                                            , stop_id=self.stop_id)
                                    , columns='stop_id, inn')
        else:
            print('WARNING: Стопы с типом {stop_type} не загружаются в репозиторий стопов'.format(
                stop_type=self.stop_type))

class StopList(object):
    '''
    Класс для фильтрации таргета стоп-факторами

    Параметры
    ----------
    object: экземпляр класса cjm.GPConnector() для подключения к БД
    stop_list_cd: str, по умолчанию None; краткое описание таргета для фильтрации, 100 символов

    Атрибуты
    ----------
    gpconnector: экземпляр класса cjm.GPConnector() для подключения к БД
    stop_list_cd: str, по умолчанию None; название списка стопов
    stop_list: список стопов

    Методы
    ----------
    create: метод для создания списка стопов;
    add_stop: метод для добавления стопа в список стопов;
    change: метод для изменения списка стопов;
    delete: метод для удаления списка стопов;
    get: метод для получения списка стопов в виде словаря;
    select: метод для вывода списка стопов на печать
    select_list: метод для вывода списка стопов в list
    select_df: метод для вывода списка стопов в датафрейм

    Таблицы
    ----------
    prom.stop_list_dict: справочник списков стопов
    '''
    def __init__(self, gpconnector, stop_list_cd: str = None):
        self.gpconnector = gpconnector
        self.stop_list_cd = stop_list_cd
        self.stop_list_dict = "prom.stop_list_dict"
        if self.stop_list_cd:
            self.stop_list = self.get()
    
    def create(self, stop_list: dict, stop_list_cd: str = None):
        '''
        Метод для создания списка стопов

        Параметры
        ----------
        stop_list: dict; словарь кодов стопов из prom.stop_dict и аргументов стопов;
        Пример: {'Наличие счета':''
               , 'Наличие ЗП1':''
               , 'stop function test':"'{3}','{ЦА-КП03-0622}','{1,2}'"
               , 'stop view test 111':''
               , 'stop create test2':''
               , 'stop create mv test2':''};
        stop_list_cd: str, по умолчанию None; название списка стопов
        '''

        check_exists = self.gpconnector.select_list("select * from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"
                                                   .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd or stop_list_cd))
        if not(self.stop_list_cd or stop_list_cd):
            raise Exception("ERROR: Укажите stop_list_cd!") 
        elif len(check_exists) > 0:
            raise Exception("ERROR: Список стопов {stop_list_cd} уже существует!".format(stop_list_cd=self.stop_list_cd or stop_list_cd)) 
        else:
            stop_list_items = [(self.stop_list_cd or stop_list_cd,) + x for x in list(stop_list.items())]
            self.gpconnector.insert(self.stop_list_dict, stop_list_items)
            self.stop_list = self.get()
            print("SUCCESS: Список стопов {stop_list_cd} создан".format(stop_list_cd=self.stop_list_cd or stop_list_cd))

    def change(self, stop_list: dict, stop_list_cd: str = None):
        '''
        Метод для изменения списка стопов: удаляет все записи по данному списку и заменяет их записями из параметра stop_list

        Параметры
        ----------
        stop_list: dict; словарь кодов стопов из prom.stop_dict и аргументов стопов;
        Пример: {'Наличие счета':''
               , 'Наличие ЗП1':''
               , 'stop function test':"'{3}','{ЦА-КП03-0622}','{1,2}'"
               , 'stop view test 111':''
               , 'stop create test2':''
               , 'stop create mv test2':''}
        stop_list_cd: str, по умолчанию None; название списка стопов
        '''

        if not(self.stop_list_cd or stop_list_cd):
            raise Exception("ERROR: Укажите stop_list_cd!")
        else:
            self.gpconnector.execute("delete from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"
                                    .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd or stop_list_cd))
            stop_list_items = [(self.stop_list_cd or stop_list_cd,) + x for x in list(stop_list.items())]
            self.gpconnector.insert(self.stop_list_dict, stop_list_items)
            self.stop_list = self.get()
            print("SUCCESS: Список стопов {stop_list_cd} изменен".format(stop_list_cd=self.stop_list_cd or stop_list_cd))

    def delete(self, stop_list_cd: str = None):
        '''
        Метод для удаления списка стопов

        Параметры
        ----------
        stop_list_cd: str, по умолчанию None; название списка стопов
        '''

        if not(self.stop_list_cd or stop_list_cd):
            raise Exception("ERROR: Укажите stop_list_cd!")
        else:
            self.gpconnector.execute("delete from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"
                                    .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd or stop_list_cd))
            self.stop_list = self.get()
            print("SUCCESS: Список стопов {stop_list_cd} очищен".format(stop_list_cd=self.stop_list_cd or stop_list_cd))

    def add_stop(self, stop_list: dict, stop_list_cd: str = None):
        '''
        Метод для добавления стопов в список стопов

        Параметры
        ----------
        stop_list: dict; словарь кодов стопов из prom.stop_dict и аргументов стопов;
        Пример: {'Наличие счета':''
               , 'Наличие ЗП1':''
               , 'stop function test':"'{3}','{ЦА-КП03-0622}','{1,2}'"
               , 'stop view test 111':''
               , 'stop create test2':''
               , 'stop create mv test2':''}
        stop_list_cd: str, по умолчанию None; название списка стопов
        '''

        if not(self.stop_list_cd or stop_list_cd):
            raise Exception("ERROR: Укажите stop_list_cd!")
        else:
            stop_list_items = [(self.stop_list_cd or stop_list_cd,) + x for x in list(stop_list.items())]
            self.gpconnector.insert(self.stop_list_dict, stop_list_items)
            self.stop_list = self.get()
            print("SUCCESS: Список стопов {stop_list_cd} изменен".format(stop_list_cd=self.stop_list_cd or stop_list_cd))

    def get(self, stop_list_cd: str = None) -> dict:
        '''
        Метод для получения списка стопов в виде словаря

        Параметры
        ----------
        stop_list_cd: str, по умолчанию None; название списка стопов
        '''

        if self.stop_list_cd or stop_list_cd:
            return dict(self.gpconnector.select_list("""select stop_cd, stop_arguments from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"""
                                                    .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd or stop_list_cd), limit = 1000))
        else:
            raise Exception("ERROR: Укажите stop_list_cd!")

    def select(self):
        '''
        Метод для вывода таргета на печать
        '''

        self.gpconnector.select("""select * from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"""
                                .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd), limit = 1000)

    def select_list(self) -> list:
        '''
        Метод для вывода списка стопов в list
        '''

        return self.gpconnector.select_list("""select * from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"""
                                            .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd), limit = 1000)

    def select_df(self) -> pd.DataFrame:
        '''
        Метод для вывода списка стопов в датафрейм
        '''

        return self.gpconnector.select_df("""select * from {stop_list_dict} where stop_list_cd = '{stop_list_cd}'"""
                                          .format(stop_list_dict=self.stop_list_dict, stop_list_cd=self.stop_list_cd), limit = 1000)    

class StopFilter(object):
    '''
    Класс для фильтрации таргета стоп-факторами

    Параметры
    ----------
    object: экземпляр класса cjm.GPConnector() для подключения к БД
    target_id: int, по умолчанию None; id таргета для фильтрации
    target_cd: str, по умолчанию None; краткое описание таргета для фильтрации, 100 символов

    Атрибуты
    ----------
    gpconnector: экземпляр класса cjm.GPConnector() для подключения к БД
    target_id: int, по умолчанию None; id таргета
    target_cd: str, по умолчанию None; краткое описание таргета, 100 символов
    create_dt: date; дата создания таргета
    refresh_dt: date; дата обновления таргета
    stop_dict: str; название таблицы со списком стопов
    stop_repository str; название таблицы с репозиторием стопов
    stop_template: str; шаблон названия стопа
    target_template: str; шаблон названия таргета
    target_flags_template: str; шаблон названия таргета с признаками срабатывания стопов

    Методы
    ----------
    create_target: метод для добавления таргета в справочник таргетов prom.stop_target_dict и создания временной таблицы с таргетом
    delete_target: метод для удаления таргета
    select_target: метод для вывода таргета на печать
    select_target_list: метод для вывода таргета в list
    select_target_df: метод для вывода таргета в датафрейм
    select_target_flags: метод для вывода таргета на печать
    select_target_flags_list: метод для вывода таргета в list
    select_target_flags_df: метод для вывода таргета в датафрейм
    exclude: метод для исключения из таргета клиентов, попадающих под указанные стопы
    show_statistics: метод для вывода статистики исключения из таргета клиентов, попадающих под указанные стопы

    Таблицы
    ----------
    prom.stop_repository: репозиторий стопов
    prom.stop_dict: справочник стопов
    prom.stop_target_dict: справочник таргетов
    sandbox.stop_target_{target_id}: таблица, содержащая таргет по данному target_id
    sandbox.stop_target_flags_{target_id}: таблица, содержащая таргет по данному target_id и признаки срабатывания стопов
    '''

    def __init__(self, gpconnector, target_id: int = None, target_cd: str = None):
        self.gpconnector = gpconnector
        self.target_id = target_id
        self.target_cd = target_cd
        self.stop_dict = "prom.stop_dict"
        self.stop_template = "prom.stop_"
        self.stop_repository = "prom.stop_repository"
        self.stop_target_dict = "prom.stop_target_dict"
        self.target_template = "sandbox.stop_target_"
        self.target_flags_template = "sandbox.stop_target_flags_"

        if not target_id and target_cd:
            self.target_id, self.create_dt, self.refresh_dt = gpconnector.select_list("""select target_id, create_dt, refresh_dt from {stop_target_dict}
                                                                                      where target_cd = '{target_cd}'""".format(
                stop_target_dict=self.stop_target_dict, target_cd=target_cd))[0]
        if not target_cd and target_id:
            self.target_cd, self.create_dt, self.refresh_dt = gpconnector.select_list("""select target_cd, create_dt, refresh_dt from {stop_target_dict}
                                                                                      where target_id = {target_id}""".format(
                stop_target_dict=self.stop_target_dict, target_id=target_id))[0]

    def create_target(self, target, target_cd: str, columns: str = None):
        '''
        Метод для добавления таргета в справочник таргетов prom.stop_target_dict и создания временной таблицы с таргетом

        Параметры
        ----------
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для фильтрации
        target_cd: str; краткое описание таргета, 100 символов
        columns: str, по умолчанию None; список колонок для таргета типа list
        '''

        if self.target_id:
            raise Exception("ERROR: Таргет с таким target_id уже существует!")
        elif self.gpconnector.select_list("select target_id from {stop_target_dict} where target_cd = '{target_cd}'"
                                          .format(stop_target_dict = self.stop_target_dict, target_cd = target_cd)):
            raise Exception("ERROR: Таргет с таким target_cd уже существует!")
        else:
            self.target_cd = target_cd or self.target_cd

            if isinstance(target, (pd.DataFrame, list)):
                if isinstance(target, pd.DataFrame):
                    target_columns = ', '.join(list(target.columns))
                    target = [tuple(x) for x in target.get_values()]
                else:
                    target_columns = None
                if not (columns or target_columns):
                    raise Exception("ERROR: Для фильтрации list/датафрейма необходимы названия колонок!")

            if self.gpconnector.validate_target(target, check_function=False):
                self.gpconnector.insert(self.stop_target_dict, [(self.target_cd,)], columns='target_cd')
                self.target_id = self.gpconnector.select_list("select last_value from {stop_target_dict}_target_id_seq"
                                                            .format(stop_target_dict=self.stop_target_dict))[0][0]
                self.gpconnector.create('TABLE', '{target_template}{target_id}'
                                        .format(target_template=self.target_template, target_id=self.target_id), target, columns)
            else:
                raise Exception("ERROR: В параметр target подан некорректный тип таргета!")

    def delete_target(self):
        '''
        Метод для удаления таргета
        '''

        self.gpconnector.drop("{target_template}{target_id}".format(target_template=self.target_template, target_id=self.target_id))
        self.gpconnector.execute("""delete from {stop_target_dict} where target_id = {target_id};"""
                                 .format(stop_target_dict=self.stop_target_dict
                                        , target_id=self.target_id))
        print('SUCCESS: Таргет №{target_id} "{target_cd}" удален'.format(target_id=self.target_id,
                                                                         target_cd=self.target_cd))
        self.target_id, self.target_cd, self.create_dt, self.refresh_dt = None, None, None, None

    def select_target(self, limit: int = 100):
        '''
        Метод для вывода таргета на печать

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        self.gpconnector.select("""select * from {target_template}{target_id}"""
                                .format(target_template=self.target_template, target_id=self.target_id), limit)

    def select_target_list(self, limit: int = 100) -> list:
        '''
        Метод для вывода таргета в list

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        return self.gpconnector.select_list("""select * from {target_template}{target_id}"""
                                            .format(target_template=self.target_template, target_id=self.target_id), limit)

    def select_target_df(self, limit: int = 100) -> pd.DataFrame:
        '''
        Метод для вывода таргета в датафрейм

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        return self.gpconnector.select_df("""select * from {target_template}{target_id}"""
                                          .format(target_template=self.target_template, target_id=self.target_id), limit)

    def select_target_flags(self, limit: int = 100):
        '''
        Метод для вывода таргета с признаками стопов на печать

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        self.gpconnector.select("""select * from {target_flags_template}{target_id}"""
                                .format(target_flags_template=self.target_flags_template, target_id=self.target_id), limit)

    def select_target_flags_list(self, limit: int = 100) -> list:
        '''
        Метод для вывода таргета с признаками стопов в list

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        return self.gpconnector.select_list("""select * from {target_flags_template}{target_id}"""
                                            .format(target_flags_template=self.target_flags_template, target_id=self.target_id), limit)

    def select_target_flags_df(self, limit: int = 100) -> pd.DataFrame:
        '''
        Метод для вывода таргета с признаками стопов в датафрейм

        Параметры
        ----------
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        return self.gpconnector.select_df("""select * from {target_flags_template}{target_id}"""
                                          .format(target_flags_template=self.target_flags_template, target_id=self.target_id), limit)

    def exclude(self, stop_list: dict, limit: int = 100) -> pd.DataFrame:
        '''
        Метод для исключения из таргета клиентов, попадающих под указанные стопы

        Параметры
        ----------
        stop_list: dict; список стопов
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        self.gpconnector.drop("""{target_flags_template}{target_id}"""
                              .format(target_flags_template=self.target_flags_template, target_id=self.target_id))

        stop_list_filter = '{\"' + '", "'.join(stop_list.keys()) + '\"}'
        stop_list_dict = np.array(self.gpconnector.select_list("""select stop_id, stop_cd, stop_type 
                                                               from {stop_dict} where stop_cd = any('{array}')"""
                                                               .format(stop_dict=self.stop_dict, array=stop_list_filter)))
        stop_repository_ids = ', '.join(
            [stop[0] for stop in stop_list_dict if stop[2] in ['MATERIALIZED VIEW', 'TABLE']])

        stops_query, pivot_stops_columns, result_stops_columns = '', '', ''

        if len(stop_repository_ids) > 0:
            stops_query = """select distinct stop_id, inn from {stop_repository} where stop_id in ({stop_ids}) union\n""" \
                .format(stop_repository=self.stop_repository, stop_ids=stop_repository_ids)

        len_stop_list_dict = len(stop_list_dict)

        for i in range(len_stop_list_dict):
            if stop_list_dict[i][2] == 'FUNCTION':
                stops_query += "select distinct {stop_id} as stop_id, inn from {stop_template}{stop_id} ({args}) union\n" \
                    .format(stop_template=self.stop_template
                            , stop_id=str(stop_list_dict[i][0])
                            , args=stop_list.get(stop_list_dict[i][1]))
            if stop_list_dict[i][2] == 'VIEW':
                stops_query += "select distinct {stop_id} as stop_id, inn from {stop_template}{stop_id} union\n" \
                    .format(stop_template=self.stop_template
                            , stop_id=str(stop_list_dict[i][0]))

            pivot_stops_columns += ", max(case when stop_id = {stop_id} then 1 else 0 end) as stop_{stop_id}\n" \
                .format(stop_id=stop_list_dict[i][0])

            result_stops_columns += ", stop_{stop_id}\n" \
                .format(stop_id=stop_list_dict[i][0])

        result_query = \
            """with stops_query as (
            {stops_query}
            ),
            pivot_stops_query as(
            select inn
            {pivot_stops_columns}   
              from stops_query
            group by inn  
            )
            select t.*
            , case when greatest({greatest_stop_columns}) = 1 then 1 else 0 end as stop 
            {result_stops_columns}
              from {target_template}{target_id} as t
               left join pivot_stops_query as s
                on t.inn = s.inn""" \
                .format(stops_query=stops_query.rstrip(' union\n')
                        , pivot_stops_columns=pivot_stops_columns.rstrip('\n')
                        , greatest_stop_columns=result_stops_columns.replace('\n', '').lstrip(', ')
                        , result_stops_columns=result_stops_columns.rstrip('\n')
                        , target_template=self.target_template
                        , target_id=self.target_id)

        self.gpconnector.create('TABLE', "{target_flags_template}{target_id}"
                                .format(target_flags_template=self.target_flags_template, target_id=self.target_id), result_query)

        return self.gpconnector.select_df("{target_flags_template}{target_id}"
                                          .format(target_flags_template=self.target_flags_template
                                                  , target_id=self.target_id), limit)

    def show_statistics(self):
        '''
        Метод для вывода статистики исключения из таргета клиентов, попадающих под указанные стопы
        '''

        df = self.select_target_flags_df(limit=1000000)
        stop_dict = dict(self.gpconnector.select_list("select 'stop_'||cast(stop_id as varchar), stop_cd from {stop_dict}"
                                                      .format(stop_dict = self.stop_dict)))
        df = df.rename(columns=stop_dict)
        df = df.replace({0: np.nan})
        df = df[['inn', 'stop']+[x for x in list(df.columns) if x in stop_dict.values()]]
        df = df.drop_duplicates()
        df_funnel = df.rename(columns={'inn': 'Таргет', 'stop': 'Всего отфильтровано'}).count().reset_index(
            name='count').rename(columns={'index': 'Уровни воронки'})
        df_funnel['sort_column'] = df_funnel['Уровни воронки'].map({'Таргет' : 2, 'Всего отфильтровано' : 1})
        df_funnel = df_funnel.sort_values(['sort_column', 'count'], ascending=False).drop('sort_column',
                                                                                          axis=1).reset_index(drop=True)
        df_funnel['Кол-во клиентов'] = 'Всего'

        stop_columns = [x for x in list(list(df_funnel['Уровни воронки'].get_values())) if x in stop_dict.values()]

        df_funnel = df_funnel.append(
            pd.DataFrame([['Таргет', df_funnel[df_funnel['Уровни воронки'] == 'Таргет']['count'].get_values()[0], 'Уникальных'],
                          ['Всего отфильтровано',
                           df_funnel[df_funnel['Уровни воронки'] == 'Всего отфильтровано']['count'].get_values()[0],
                           'Уникальных']]
                         , columns=['Уровни воронки', 'count', 'Кол-во клиентов']), ignore_index=True, sort=False)
        for i in range(len(stop_columns)):
            inn_distinct = set()
            if i == 0:
                inn_distinct = inn_distinct | set(df[df[stop_columns[i]] == 1]['inn'])
            else:
                inn_distinct = inn_distinct | set(
                    df[df[stop_columns[:i]].isna().all(axis='columns') & df[stop_columns[i]] == 1]['inn'])
            df_funnel = df_funnel.append(pd.DataFrame([[stop_columns[i], len(inn_distinct), 'Уникальных']],
                                                      columns=['Уровни воронки', 'count', 'Кол-во клиентов']), ignore_index=True,
                                         sort=False)

        stop_cross_labels = [x for xs in
                             [[x for x in stop_columns[:i] + stop_columns[i + 1:]] for i in range(len(stop_columns))]
                             for x in xs]
        stop_cross_counts = [x[0] for xs in
                             [df[stop_columns].groupby(x).count().transpose().get_values() for x in stop_columns] for x
                             in
                             xs]
        label = ['Таргет: {x}'.format(x=int(df['inn'].count())) + '; Кол-во клиентов: {x}'.format(
            x=int(df['stop'].count()))] \
                + [x + ': {count}'.format(count=int(df[x].sum())) for x in stop_columns] \
                + ["{}: {}".format(labels_, counts_) for labels_, counts_ in zip(stop_cross_labels, stop_cross_counts)]
        values = [x[0] for xs in [df[stop_columns].groupby(x).count().transpose().get_values() for x in stop_columns]
                  for x in xs]

        stop_columns_indexes = [i for i in range(1, len(stop_columns) + 1)]
        node_color_indexes = [i for i in range(len(stop_columns) + 1)] \
                             + [x for xs in [[x for x in stop_columns_indexes[:i - 1] \
                                              + stop_columns_indexes[i:]] for i in stop_columns_indexes] for x in xs]

        fig0 = px.funnel(df_funnel,
                         x='count',
                         y='Уровни воронки',
                         color='Кол-во клиентов',
                         title='Воронка стопов таргета "{target_cd}"'.format(target_cd=self.target_cd), opacity=0.9)
        fig0.show()

        fig1 = go.Figure(data=[go.Sankey(
            node=dict(label=label
                      , color=['rgba' + str(
                    ImageColor.getcolor(px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)],
                                        'RGB')).replace(')', ', 0.8)') for i in
                               node_color_indexes]),
            link=dict(
                source=[0] * len(stop_columns)
                       + [x for xs in [[i] * (len(stop_columns) - 1) for i in range(1, len(stop_columns) + 1)] for x in
                          xs]
                , target=[i for i in range(1, len(label))]
                , value=[int(df[x].sum()) for x in stop_columns]
                        + stop_cross_counts
                , color=['rgba' + str(
                    ImageColor.getcolor(px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)],
                                        'RGB')).replace(')', ', 0.5)') for i in
                         [0] * len(stop_columns) + [x for xs in [[i] * (len(stop_columns) - 1) for i in
                                                                 range(1, len(stop_columns) + 1)] for x in xs]]
            ))])
        fig1.update_layout(title='Пересечения стопов таргета "{target_cd}"'.format(target_cd=self.target_cd))
        fig1.show()

        # fig2 = make_subplots(rows=1, cols=2, shared_xaxes=True, shared_yaxes=True, vertical_spacing=0.001)
        #
        # for x in stop_columns:
        #
        #     fig2.append_trace(go.Bar(
        #         y=[x],
        #         x=[df[x].sum()],
        #         showlegend=False,
        #         name=x,
        #         orientation='h', ), 1, 2)
        #
        #     df_counts = df[stop_columns].groupby(x).count().transpose().get_values()
        #     if df_counts.shape[1] > 0:
        #         df_counts = [x[0] for x in df_counts]
        #     else:
        #         df_counts = [0] * len(stop_columns)
        #     fig2.append_trace(go.Bar(
        #         y=[y for y in stop_columns if y != x],
        #         x=df_counts,
        #         name=x,
        #         orientation='h', ), 1, 1)
        #
        # fig2.update_layout(barmode='stack')
        # fig2.update_layout(
        #     legend=dict(yanchor='bottom', y=-0.7, xanchor='left', x=0.1),
        #     hoverlabel=dict(font_size=12),
        #     title='Статистика отсечения таргета "{target_cd}" стоп-факторами'.format(target_cd=self.target_cd),
        #     xaxis=dict(title='Кол-во пересечений с другими стопами', dtick=1),
        #     xaxis2=dict(title='Общее количество клиентов в стопе', dtick=1))
        # fig2.update_traces(hoverinfo="all", hovertemplate="%{x}", col=1)
        # fig2.update_traces(hoverinfo="all", hovertemplate="%{x}", col=2)
        # fig2.update_layout(hovermode="y unified")
        # fig2.show()