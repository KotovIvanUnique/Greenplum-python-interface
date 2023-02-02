import pandas as pd
import psycopg2
from psycopg2 import Error
from multipledispatch import dispatch
import numpy as np
from matplotlib_venn import venn2, venn3
from matplotlib import pyplot as plt, rcParams
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from PIL import ImageColor
import sqlparse
from datetime import datetime

class Util:
    '''
    Класс с полезными функциями

    Методы
    ----------
    format_inn: метод для форматирования ИНН
    format_sql: метод для форматирования SQL-кода
    '''

    @staticmethod
    def format_inn(inn) -> str:
        '''
        Метод для для форматирования ИНН

        Параметры
        ----------
        inn: number или str; ИНН
        '''

        inn = str(inn)

        if len(inn) <= 9:
            inn = inn.zfill(10)
        elif len(inn) == 11:
            inn = inn.zfill(12)

        return inn

    @staticmethod
    def format_sql(sql: str) -> str:
        '''
        Метод для для форматирования SQL-кода

        Параметры
        ----------
        sql: str; sql-код
        '''

        return sqlparse.format(sql, reindent_aligned=True, use_space_around_operators=True, keyword_case='upper', identifier_case='lower')

class GPConnector():
    '''
    Класс для соединения с ПКАП CJM

    Параметры
    ----------
    user: имя пользователя
    password: пароль пользователя
    host: хост-адрес базы данных
    dbname: имя базы данных

    Атрибуты
    ----------
    user: str; имя пользователя
    password: str; пароль пользователя
    host: str; хост-адрес базы данных
    dbname: str; имя базы данных

    Методы
    ----------
    select: метод для вывода результатов запроса на печать
    select_list: метод для выбора данных в list
    select_df: метод для выбора данных в датафрейм
    get_object_type: метод для получения типа объекта
    get_source_code: метод для вывода исходного кода объекта на печать
    explain: метод для вывода плана запроса
    validate_target: метод для проверки корректности таргета
    execute: метод для запуска скриптов
    create: метод для создания объектов
    truncate: метод для очистки таблиц
    insert: метод для вставки записей в таблицу
    refresh: метод для обновления объекта
    drop: метод для удаления объектов
    '''

    def __init__(self, user: str = 'postgres', password: str = '1234', host: str = 'localhost',
                 dbname: str = 'postgres'):
        self.user = user
        self.password = password
        self.host = host
        self.dbname = dbname

    def select(self, query, limit=100, options: str = None):
        '''
        Метод для вывода результатов запроса на печать

        Параметры
        ----------
        query: sql-запрос или название таблицы в формате схема.название
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
            if 'select' not in query.lower():
                print(pd.read_sql("""select * from {query} limit {limit}""".format(query=query, limit=limit),
                                  conn).to_string(index=False))
            else:
                print(pd.read_sql("""{query} limit {limit}""".format(query=query, limit=limit), conn).to_string(
                    index=False))

    def select_list(self, query: str, limit: int = 1000000, options: str = None) -> list:
        '''
        Метод для выбора данных в list

        Параметры
        ----------
        query: str; sql-запрос
        limit: int, по умолчанию 1000000; ограничение количества выводимых строк
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
            with conn.cursor() as cur:
                if 'select' not in query.lower():
                    cur.execute("""select * from {query} limit {limit}""".format(query=query, limit=limit))
                else:
                    cur.execute("""{query} limit {limit}""".format(query=query, limit=limit))
                return cur.fetchall()

    def select_df(self, query: str, limit: int = 1000000, options: str = None) -> pd.DataFrame:
        '''
        Метод для выбора данных в датафрейм

        Параметры
        ----------
        query: str; sql-запрос
        limit: int, по умолчанию 1000000; ограничение количества выводимых строк
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
            if 'select' not in query.lower():
                return pd.read_sql("""select * from {query} limit {limit}""".format(query=query, limit=limit), conn)
            else:
                return pd.read_sql("""{query} limit {limit}""".format(query=query, limit=limit), conn)

    def execute(self, script: str, data=None, options: str = None):
        '''
        Метод для запуска скриптов

        Параметры
        ----------
        script: str; sql-код скрипта
        data: датафрейм или list; данные для скрипта
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        @dispatch(str, object)
        def execute_dispatch(script, options):
            with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

        @dispatch(str, pd.DataFrame, object)
        def execute_dispatch(script, data, options):
            data = [tuple(x) for x in data.get_values()]

            with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
                with conn.cursor() as cur:
                    cur.execute(script, data)

        @dispatch(str, list, object)
        def execute_dispatch(script, data, options):
            with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
                with conn.cursor() as cur:
                    cur.execute(script, data)

        if data is not None:
            execute_dispatch(script, data, options)
        else:
            execute_dispatch(script, options)

    def get_object_type(self, object_name: str) -> str:
        '''
        Метод для получения типа объекта

        Параметры
        ----------
        object_name: str; название объекта в формате схема.название
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        object_schema_tmp, object_name_tmp = object_name.split('.')
        object_type = self.select_list(
            """select max(object_type) from prom.pg_objects where schemaname = '{object_schema}' and objectname = '{object_name}'"""
            .format(object_schema=object_schema_tmp, object_name=object_name_tmp))
        if len(object_type) > 0:
            return object_type[0][0]

    def get_source_code(self, object_name: str) -> str:
        '''
        Метод для вывода исходного кода объекта на печать

        object_name: str; название объекта в формате схема.название
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        object_type = self.get_object_type(object_name)
        object_schema_tmp, object_name_tmp = object_name.split('.')

        if object_type == 'TABLE':
            print('WARNING: Объект {object_name} является таблицей'.format(object_name=object_name))
        else:
            source_code = self.select_df("""select max(definition) from prom.pg_objects 
                                         where schemaname = '{object_schema}' and objectname = '{object_name}' and object_type = '{object_type}'"""
                                         .format(object_schema=object_schema_tmp
                                                 , object_name=object_name_tmp
                                                 , object_type=object_type)).get_values()[0][0].strip()

            if source_code is not None:
                return source_code
            else:
                print('WARNING: Исходный код не найден')

    def create(self, object_type: str, object_name: str, target, columns: str = None, options: str = None):
        '''
        Метод для создания объектов

        Параметры
        ----------
        object_type: str, {MATERIALIZED VIEW', 'VIEW', 'TABLE', 'FUNCTION'}; тип объекта
        object_name: str; название объекта в формате схема.название
        target: sql-код запроса
            или название таблицы в формате схема.название
            или sql-функция (должна начинаться с 'create or replace function {function_name}')
            или list
            или датафрейм;
            данные для создания объекта
        columns: str, по умолчанию None; список колонок в созданной таблице в формате 'col1, col2, col3'
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        @dispatch(str, str, pd.DataFrame, object)
        def create_dispatch(object_type, object_name, target, options):
            target_columns = ', '.join(list(target.columns))
            target = [tuple(x) for x in target.get_values()]

            target_records = ", ".join(["%s"] * len(target))

            self.execute("""create {object_type} {object_name} as
                         select {columns} from (VALUES {target_records}) AS t ({columns})"""
                         .format(columns=target_columns
                                 , object_type=object_type
                                 , object_name=object_name
                                 , target_records=target_records), target, options=options)

        @dispatch(str, str, pd.DataFrame, str, object)
        def create_dispatch(object_type, object_name, target, columns, options):
            target_columns = ', '.join(list(target.columns))
            target = [tuple(x) for x in target.get_values()]

            target_records = ", ".join(["%s"] * len(target))

            self.execute("""create {object_type} {object_name} as
                         select {columns} from (VALUES {target_records}) AS t ({target_columns})"""
                         .format(columns=columns
                                 , target_columns=target_columns
                                 , object_type=object_type
                                 , object_name=object_name
                                 , target_records=target_records), target, options=options)

        @dispatch(str, str, list, str, object)
        def create_dispatch(object_type, object_name, target, columns, options):
            target_records = ", ".join(["%s"] * len(target))

            self.execute("""create {object_type} {object_name} as
                         select {columns} from (VALUES {target_records}) AS t ({columns})"""
                         .format(columns=columns
                                 , object_type=object_type
                                 , object_name=object_name
                                 , target_records=target_records), target, options=options)

        @dispatch(str, str, str, object)
        def create_dispatch(object_type, object_name, target, options):
            if object_type.upper() == 'FUNCTION':
                if 'create or replace function {function_name}' not in target.lower():
                    raise Exception(
                        "ERROR: Объект с типом 'FUNCTION' должен начинаться с 'create or replace function {function_name}'!")
                else:
                    self.execute(target.format(function_name=object_name), options=options)
            else:
                if 'select' not in target.lower():
                    self.execute("""create {object_type} {object_name} as
                                 select * from {source_name}""".format(object_type=object_type
                                                                       , source_name=target
                                                                       , object_name=object_name), options=options)
                else:
                    self.execute("""create {object_type} {object_name} as
                                 {sql}""".format(object_type=object_type, object_name=object_name, sql=target), options=options)

        @dispatch(str, str, str, str, object)
        def create_dispatch(object_type, object_name, target, columns, options):
            if object_type.upper() == 'FUNCTION':
                if 'create or replace function {function_name}' not in target.lower():
                    raise Exception(
                        "ERROR: Объект с типом 'FUNCTION' должен начинаться с 'create or replace function {function_name}'!")
                else:
                    self.execute(target.format(function_name=object_name), options=options)
            else:
                if 'select' not in target.lower():
                    self.execute("""create {object_type} {object_name} as
                                 select {columns} from {source_name}""".format(object_type=object_type
                                                                               , source_name=target
                                                                               , columns=columns
                                                                               , object_name=object_name), options=options)
                else:
                    self.execute("""create {object_type} {object_name} as
                                 select {columns} from ({sql}) as t""".format(object_type=object_type
                                                                              , object_name=object_name
                                                                              , sql=target
                                                                              , columns=columns), options=options)

        self.drop(object_name)

        if columns:
            create_dispatch(object_type, object_name, target, columns, options)
        else:
            create_dispatch(object_type, object_name, target, options)

        print('SUCCESS: Объект {object_type} {object_name} создан'.format(object_type=object_type.upper()
                                                                          , object_name=object_name))

    def insert(self, table_name: str, target, columns: str = None, options: str = None):
        '''
        Метод для вставки записей в таблицу

        Параметры
        ----------
        table_name: str; название таблицы для загрузки в формате схема.название
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для создания объекта
        columns: str; названия колонок для заполнения в формате 'col1, col2, col3'
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        @dispatch(str, pd.DataFrame, object)
        def insert_dispatch(table_name, target, options):
            target_columns = ', '.join(list(target.columns))
            target = [tuple(x) for x in target.get_values()]

            target_records = ", ".join(["%s"] * len(target))

            self.execute("""insert into {table_name} ({columns})
                         select {columns} from (VALUES {target_records}) as t ({columns})"""
                         .format(columns=target_columns
                                 , table_name=table_name
                                 , target_records=target_records), target, options=options)

        @dispatch(str, pd.DataFrame, str, object)
        def insert_dispatch(table_name, target, columns, options):
            target_columns = ', '.join(list(target.columns))
            target = [tuple(x) for x in target.get_values()]

            target_records = ", ".join(["%s"] * len(target))

            self.execute("""insert into {table_name} ({columns})
                         select {columns} from (VALUES {target_records}) as t ({target_columns})"""
                         .format(table_name=table_name
                                 , columns=columns
                                 , target_records=target_records
                                 , target_columns=target_columns), target, options=options)

        @dispatch(str, list, object)
        def insert_dispatch(table_name, target, options):
            target_records = ", ".join(["%s"] * len(target))

            self.execute("""insert into {table_name}
                         values {target_records}""".format(table_name=table_name, target_records=target_records),
                         target, options=options)

        @dispatch(str, list, str, object)
        def insert_dispatch(table_name, target, columns, options):
            target_records = ", ".join(["%s"] * len(target))

            self.execute("""insert into {table_name} ({columns})
                         values {target_records}""".format(table_name=table_name
                                                           , columns=columns
                                                           , target_records=target_records), target, options=options)

        @dispatch(str, str, object)
        def insert_dispatch(table_name, target, options):
            if 'select' not in target.lower():
                self.execute("""insert into {table_name}
                             select * from {source_name}""".format(table_name=table_name, source_name=target), options=options)
            else:
                self.execute("""insert into {table_name}
                             {sql}""".format(table_name=table_name, sql=target, options=options))

        @dispatch(str, str, str, object)
        def insert_dispatch(table_name, target, columns, options):
            if 'select' not in target.lower():
                self.execute("""insert into {table_name} ({columns})
                             select {columns} from {source_name}""".format(table_name=table_name
                                                                           , columns=columns
                                                                           , source_name=target), options=options)
            else:
                self.execute("""insert into {table_name} ({columns})
                             select {columns} from ({sql}) as t"""
                             .format(table_name=table_name
                                     , columns=columns
                                     , sql=target), options=options)

        if columns:
            insert_dispatch(table_name, target, columns, options)
        else:
            insert_dispatch(table_name, target, options)

        print('SUCCESS: Вставка записей в таблицу {table_name} завершена'.format(table_name=table_name))

    def truncate(self, table_name: str, options: str = None):
        '''
        Метод для очистки таблиц

        Параметры
        ----------
        table_name: str; название таблицы в формате схема.название
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        self.execute("truncate {table_name}".format(table_name=table_name), options=options)
        print('SUCCESS: Таблица {table_name} очищена'.format(table_name=table_name))

    def drop(self, object_name: str, options: str = None):
        '''
        Метод для удаления объектов

        Параметры
        ----------
        object_name: str; название объекта в формате схема.название
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        object_type = self.get_object_type(object_name)
        if object_type:
            self.execute("""drop {object_type} if exists {object_name} restrict""".format(object_type=object_type,
                                                                                          object_name=object_name)
                         , options=options)
            print('SUCCESS: Объект {object_type} {object_name} удален'.format(object_type=object_type.upper(),
                                                                              object_name=object_name))

    def refresh(self, object_name: str, options: str = None):
        '''
        Метод для обновления объекта

        Параметры
        ----------
        object_name: str; название объекта в формате схема.название
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        object_type = self.get_object_type(object_name)
        if object_type == 'MATERIALIZED VIEW':
            self.execute("refresh materialized view {object_name}".format(object_name=object_name), options=options)
            print('SUCCESS: MATERIALIZED VIEW {object_name} обновлена'.format(object_name=object_name))
        else:
            print('WARNING: Объект {object_name} с типом {object_type} не обновляется этим методом'.format(
                object_name=object_name, object_type=object_type.upper()))

    def explain(self, query: str, analyze: bool = False, options: str = None):
        '''
        Метод для вывода плана запроса

        Параметры
        ----------
        query: str; sql-запрос
        analyze: bool, по умолчанию False; указывает необходимо ли выполнить запрос для отображения действительного времени  работы и количества строк
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        with psycopg2.connect(user=self.user, password=self.password, host=self.host, dbname=self.dbname, options=options) as conn:
            if analyze:
                plan = pd.read_sql('explain analyze {query}'.format(query=query), conn)
            else:
                plan = pd.read_sql('explain {query}'.format(query=query), conn)
            print('\n'.join([x for x in plan['QUERY PLAN']]))

    def validate_target(self
                        , target
                        , check_df: bool = True
                        , check_list: bool = True
                        , check_function: bool = True
                        , check_table: bool = True
                        , check_sql: bool = True
                        , options: str = None) -> bool:
        '''
        Метод для проверки корректности таргета

        Параметры
        ----------
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для создания объекта
        check_df: bool, по умолчанию True; указывает, что разрешен тип датафрейм
        check_list: bool, по умолчанию True; указывает, что разрешен тип list
        check_function: bool, по умолчанию True; указывает, что разрешен тип function
        check_table: bool, по умолчанию True; указывает, что разрешен тип table
        check_sql: bool, по умолчанию True; указывает, что разрешен тип sql
        options: str, по умолчанию None; опции подключения к ДБ
        '''

        @dispatch(pd.DataFrame, bool, bool, bool, bool, bool, object)
        def validate_target_dispatch(target, check_df, check_list, check_function, check_table, check_sql, options):
            if check_df:
                return True
            else:
                return False

        @dispatch(list, bool, bool, bool, bool, bool, object)
        def validate_target_dispatch(target, check_df, check_list, check_function, check_table, check_sql, options):
            if check_list:
                return True
            else:
                return False

        @dispatch(str, bool, bool, bool, bool, bool, object)
        def validate_target_dispatch(target, check_df, check_list, check_function, check_table, check_sql, options):
            if 'create or replace function' in target.lower() and check_function:
                return True
            elif 'select' not in target.lower() and check_table:
                try:
                    self.execute("""explain select * from {target}""".format(target=target), options=options)
                    return True
                except psycopg2.Error:
                    return False
            elif check_sql:
                try:
                    self.execute("""explain {target}""".format(target=target), options=options)
                    return True
                except psycopg2.Error:
                    return False
            else:
                return False

        @dispatch(object, bool, bool, bool, bool, bool, object)
        def validate_target_dispatch(target, check_df, check_list, check_function, check_table, check_sql, options):
            return False

        return validate_target_dispatch(target, check_df, check_list, check_function, check_table, check_sql, options)

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

class Autocheck(object):
    '''
    Класс для использования автопроверок

    Параметры
    ----------
    object: экземпляр класса cjm.GPConnector() для подключения к БД
    target: sql-код запроса
        или название таблицы в формате схема.название
        или list
        или датафрейм;
        данные для фильтрации
    columns: str, по умолчанию None; список колонок для таргета типа list

    Атрибуты
    ----------
    gpconnector: экземпляр класса cjm.GPConnector() для подключения к БД
    target: sql-код запроса
        или название таблицы в формате схема.название
        или list
        или датафрейм;
        данные для фильтрации
    columns: str, по умолчанию None; список колонок для таргета типа list
    correct: датафрейм; записи из таргета, прошедшие проверку
    errors: датафрейм; записи из таргета, не прошедшие проверку

    Методы
    ----------
    check: метод для проверки таргета
    show_statistics: метод для вывода статистики проверки таргета

    Таблицы
    ----------
    prom.ma_deal: витрина со сделками из CMDM
    prom.ma_product_offer: витрина с продуктовыми предложениями из CMDM
    prom.ma_task: витрина с задачами из CMDM
    prom.ma_agreement: витрина с договорами из CMDM
    prom.ma_unified_customer: витрина с атрибутами организации из CMDM
    '''

    def __init__(self, gpconnector, target, columns: str = None):
        self.autocheck_template = "sandbox.autocheck_"
        self.gpconnector = gpconnector
        self.target = target
        self.columns = columns
        self.correct = None
        self.error = None
        self.deal = "prom.ma_deal"
        self.product_offer = "prom.ma_product_offer"
        self.task = "prom.ma_task"
        self.agreement = "prom.ma_agreement"
        self.unified_customer = "prom.ma_unified_customer"
        self.request_segment = "prom.request_segment"

        self.check_attributes_join = ""
        self.check_attributes_column = "case when {check_attributes} = True then 1 end"
        self.check_dates_column = "case when {check_attributes} = True then 1 end"
        self.check_segment_join = ""
        self.check_segment_column = "case when {check_segment} = True then 1 end"
        self.check_double_join = ""
        self.check_double_column = "case when {check_double} = True then 1 end"
        self.check_product_task_join = ""
        self.check_product_task_column = "case when {check_product_task} = True then 1 end"
        self.check_product_prpr_join = ""
        self.check_product_prpr_column = "case when {check_product_prpr} = True then 1 end"
        self.check_product_deal_join = ""
        self.check_product_deal_column = "case when {check_product_deal} = True then 1 end"
        self.check_product_agr_join = ""
        self.check_product_agr_column = "case when {check_product_agr} = True then 1 end"

        self.check_columns = ['Ошибки', 'Дубли', 'Незаполненные атрибуты', 'Некорректные даты', 'Задача по продукту Т-90', 'ПрПр по продукту Т-90', 'Сделка по продукту', 'Договор по продукту', 'Некорректная длина ИНН', 'Неактивный клиент', 'Некорректный сегмент']

    def check(self
              , check_double: bool = True
              , check_attributes: bool = False
              , check_product_task: bool = True
              , check_product_prpr: bool = True
              , check_product_deal: bool = True
              , check_product_agr: bool = True
              , check_inn_len: bool = True
              , check_active: bool = True
              , check_segment: bool = True
              , limit: int = 100) -> pd.DataFrame:
        '''
        Метод для проверки таргета
        Корректные строки сохраняются в атрибут-датафрейм correct, а строки с ошибками сохраняются в атрибут-датафрейм error
        После проверок на печать выводится сообщение с информацией о том, сколько строк не прошли по той или иной проверке
        Проверки:
            -Дубли
            -Незаполненные атрибуты
            -Некорректные даты начала/окончания действия инсайта
            -Задача по продукту Т-90
            -ПрПр по продукту Т-90
            -Сделка по продукту
            -Договор по продукту
            -Некорректная длина ИНН
            -Неактивный клиент
            -Некорректный сегмент

        Параметры
        ----------
        check_double: bool, по умолчанию True; указывает нужно ли использовать проверку на наличие дублей по связке ИНН-продукт
        check_attributes: bool, по умолчанию True; указывает нужно ли использовать проверку на корректность атрибутов репозитория инсайтов
        check_product_task: bool, по умолчанию True; указывает нужно ли использовать проверку на наличие задач по продукту за Т-90
        check_product_prpr: bool, по умолчанию True; указывает нужно ли использовать проверку на наличие ПрПр по продукту за Т-90
        check_product_deal: bool, по умолчанию True; указывает нужно ли использовать проверку на наличие сделки по продукту
        check_product_agr: bool, по умолчанию True; указывает нужно ли использовать проверку на наличие договора по продукту
        check_inn_len: bool, по умолчанию True; указывает нужно ли использовать проверку на длину ИНН
        check_active: bool, по умолчанию True; указывает нужно ли использовать проверку на активность клиента
        check_segment: bool, по умолчанию True; указывает нужно ли использовать проверку на соответствие сегмента клиента и сегмента из заявки
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''
        # TODO: 
        # выбор типа проверок в зависимости от целевого репозитория
        # реализовать вариант проверок для загрузки в SAS

        def set_check_product_sql(self):
            self.check_double_column = """case when {check_double} = True and row_number() over (partition by t.inn, t.product_id order by t.inn, t.product_id) > 1 then 1 end"""
            self.check_product_task_join = """left join {task} as ts on ts.inn = t.inn and ts.host_prod_id = t.product_id and ts.create_dt >= current_date - 90"""
            self.check_product_task_column = """case when {check_product_task} = True and ts.inn is not null then 1 end"""
            self.check_product_prpr_join = """left join {product_offer} as po on po.inn = t.inn and po.host_prod_id = t.product_id and po.creation_dttm >= current_date - 90"""
            self.check_product_prpr_column = """case when {check_product_prpr} = True and po.inn is not null then 1 end"""
            self.check_product_deal_join = """left join {deal} as d on d.inn = t.inn and d.host_prod_id = t.product_id and d.deal_status_nm = 'Заключена'"""
            self.check_product_deal_column = """case when {check_product_deal} = True and d.inn is not null then 1 end"""
            self.check_product_agr_join = """left join {agreement} as a on a.inn = t.inn and a.host_prod_id = t.product_id and a.active_flg = 1"""
            self.check_product_agr_column = """case when {check_product_agr} = True and a.inn is not null then 1 end"""

        def set_check_segment_sql(self):
            self.check_segment_join = """left join {request_segment} as rs on rs.request_id = t.request_id"""
            self.check_segment_column = """case when {check_segment} = True and (max(case when rs.crm_segment_type_nm = uc.crm_segment_type_nm then 1 end) over (partition by t.inn) != 1) then 1 end"""

        def set_check_attributes_sql(self):
            self.check_attributes_column = """case when {check_attributes} = True and (t.request_id is null or t.scenario_id is null or t.inn is null or t.kpp is null or t.customer_id is null or t.epk_id is null or t.product_id is null or t.insight_desc is null or t.insight_sum_val is null or t.insight_income_val is null or t.insight_start_dt is null or t.insight_end_dt is null) then 1 end"""
            self.check_dates_column = """case when {check_attributes} = True and (t.insight_start_dt < current_date or t.insight_end_dt <= current_date) then 1 end"""

        @dispatch(pd.DataFrame)
        def check_dispatch(target):
            target_columns = ', '.join(list(target.columns))

            if 'product_id' in target_columns.lower():
                set_check_product_sql(self)
            if 'request_id' in target_columns.lower():
                set_check_segment_sql(self)
            if all(elem in target_columns.upper() for elem in ['request_id', 'scenario_id', 'inn', 'kpp', 'customer_id', 'epk_id', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']):
                set_check_attributes_sql(self)

        @dispatch(pd.DataFrame, str)
        def check_dispatch(target, columns):
            target_columns = ', '.join(list(target.columns))

            if 'product_id' in columns.lower() and 'product_id' in target_columns.lower():
                set_check_product_sql(self)
            if 'request_id' in columns.lower() and 'request_id' in target_columns.lower():
                set_check_segment_sql(self)
            if all(elem in columns.lower() for elem in ['request_id', 'scenario_id', 'inn', 'kpp', 'customer_id', 'epk_id', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']) and all(elem in target_columns.lower() for elem in ['inn']):
                set_check_attributes_sql(self)

        @dispatch(list, str)
        def check_dispatch(target, columns):
            if 'product_id' in columns.lower():
                set_check_product_sql(self)
            if 'request_id' in columns.lower():
                set_check_segment_sql(self)
            if all(elem in columns.lower() for elem in ['request_id', 'scenario_id', 'inn', 'kpp', 'customer_id', 'epk_id', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']):
                set_check_attributes_sql(self)

        @dispatch(str)
        def check_dispatch(target):
            if 'select' not in target.lower():
                if self.gpconnector.validate_target('select product_id from {source_name}'.format(source_name=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from {source_name}'.format(source_name=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from {source_name}'.format(source_name=target), check_function=False):
                    set_check_attributes_sql(self)
            else:
                if self.gpconnector.validate_target('select product_id from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_attributes_sql(self)

        @dispatch(str, str)
        def check_dispatch(target, columns):
            if 'select' not in target.lower():
                if self.gpconnector.validate_target('select product_id from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_attributes_sql(self)
            else:
                if self.gpconnector.validate_target('select product_id from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
                    set_check_attributes_sql(self)

        if self.columns:
            check_dispatch(self.target, self.columns)
        else:
            check_dispatch(self.target)

        autocheck_id = self.gpconnector.select_list("select nextval('sandbox.autocheck'::regclass)")[0][0]
        self.gpconnector.create('TABLE', '{autocheck_template}{autocheck_id}'
                                .format(autocheck_template=self.autocheck_template, autocheck_id=autocheck_id)
                                , self.target, self.columns)

        result_query = \
            """
            select distinct
                   t.*
                 , case when check_double = 1
                         or check_attributes = 1
                         or check_dates = 1
                         or check_product_task = 1
                         or check_product_prpr = 1
                         or check_product_deal = 1
                         or check_product_agr = 1
                         or check_inn_len = 1
                         or check_active = 1
                         or check_segment = 1
                        then 1
                   end as check_error
              from (select t.*
                         , {check_attributes_column} as check_attributes
                         , {check_dates_column} as check_dates
                         , {check_product_task_column} as check_product_task
                         , {check_product_prpr_column} as check_product_prpr
                         , {check_product_deal_column} as check_product_deal
                         , {check_product_agr_column} as check_product_agr
                         , case when {check_inn_len} = True and length(t.inn) not in (10, 12) then 1 end as check_inn_len
                         , case when {check_active} = True and (uc.active_flg = 0 or uc.active_flg is null) then 1 end as check_active
                         , {check_segment_column} as check_segment
                      from (select t.*
                                 , {check_double_column} as check_double
                              from {autocheck_template}{autocheck_id} as t) as t
                       {check_product_deal_join}
                       {check_product_task_join}
                       {check_product_prpr_join}
                       {check_product_agr_join}
                       {check_segment_join}
                       left join {unified_customer} as uc
                        on uc.inn = t.inn) as t
            """ \
                .format(check_double_column=self.check_double_column.format(check_double=check_double)
                      , check_attributes_column=self.check_attributes_column.format(check_attributes=check_attributes)
                      , check_dates_column=self.check_dates_column.format(check_attributes=check_attributes)
                      , check_product_task_join=self.check_product_task_join.format(task=self.task)
                      , check_product_task_column=self.check_product_task_column.format(check_product_task=check_product_task)
                      , check_product_prpr_join=self.check_product_prpr_join.format(product_offer=self.product_offer)
                      , check_product_prpr_column=self.check_product_prpr_column.format(check_product_prpr=check_product_prpr)
                      , check_product_deal_join=self.check_product_deal_join.format(deal=self.deal)
                      , check_product_deal_column=self.check_product_deal_column.format(check_product_deal=check_product_deal)
                      , check_product_agr_join=self.check_product_agr_join.format(agreement=self.agreement)
                      , check_product_agr_column=self.check_product_agr_column.format(check_product_agr=check_product_agr)
                      , check_segment_join=self.check_segment_join.format(request_segment=self.request_segment)
                      , check_segment_column=self.check_segment_column.format(check_segment=check_segment)
                      , check_inn_len=check_inn_len
                      , check_active=check_active
                      , autocheck_template=self.autocheck_template
                      , autocheck_id=autocheck_id
                      , unified_customer=self.unified_customer)

        df_check = self.gpconnector.select_df(result_query, limit)
        df_check = df_check.rename(columns={'check_error' : 'Ошибки'
                                          , 'check_double' : 'Дубли'
                                          , 'check_attributes' : 'Незаполненные атрибуты'
                                          , 'check_dates' : 'Некорректные даты'
                                          , 'check_product_task' : 'Задача по продукту Т-90'
                                          , 'check_product_prpr' : 'ПрПр по продукту Т-90'
                                          , 'check_product_deal' : 'Сделка по продукту'
                                          , 'check_product_agr' : 'Договор по продукту'
                                          , 'check_inn_len' : 'Некорректная длина ИНН'
                                          , 'check_active' : 'Неактивный клиент'
                                          , 'check_segment' : 'Некорректный сегмент'})
        df_check_columns = df_check.columns.tolist()
        df_check = df_check[[x for x in df_check_columns if x not in self.check_columns] + self.check_columns]

        self.gpconnector.drop("{autocheck_template}{autocheck_id}"
                              .format(autocheck_template=self.autocheck_template, autocheck_id=autocheck_id))

        self.errors = df_check[df_check['Ошибки'] == 1]
        self.correct = df_check[df_check['Ошибки'] != 1]
        self.correct = self.correct.drop(self.check_columns, axis = 1)

        check_double_count, check_attributes_count, check_dates_count, check_product_task_count, check_product_prpr_count, check_product_deal_count, check_product_agr_count, check_inn_len_count, check_active_count, check_segment_count = \
            df_check[self.check_columns[1:]].count()

        if check_double_count > 0:
            print("WARNING: Количество записей с дублями: {count}".format(count=check_double_count))
        if check_attributes_count > 0:
            print("WARNING: Количество записей с незаполненными атрибутами: {count}".format(count=check_attributes_count))
        if check_dates_count > 0:
            print("WARNING: Количество записей с некорректно заполненными датами начала/окончания: {count}".format(count=check_dates_count))    
        if check_product_task_count > 0:
            print("WARNING: Количество записей с продуктами, по которым у клиента была задача за последние 90 дней: {count}".format(count=check_product_task_count))
        if check_product_prpr_count > 0:
            print("WARNING: Количество записей с продуктами, по которым у клиента было ПрПр за последние 90 дней: {count}".format(count=check_product_prpr_count))
        if check_product_deal_count > 0:
            print("WARNING: Количество записей с продуктами, по которым у клиента была сделка: {count}".format(count=check_product_deal_count))
        if check_product_agr_count > 0:
            print("WARNING: Количество записей с продуктами, по которым у клиента есть договор: {count}".format(count=check_product_agr_count))
        if check_inn_len_count > 0:
            print("WARNING: Количество записей с ИНН некорректной длины: {count}".format(count=check_inn_len_count))
        if check_active_count > 0:
            print("WARNING: Количество записей с неактивными клиентами: {count}".format(count=check_active_count))
        if check_segment_count > 0:
            print("WARNING: Количество записей с некорректным сегментом: {count}".format(count=check_segment_count))
        if len(self.errors) == 0:
            print("SUCCESS: Все проверки пройдены успешно")

        return self.correct

    def show_statistics(self):
        '''
        Метод для вывода статистики проверки таргета
        '''

        df = pd.concat([self.correct, self.errors], sort=False)
        df = df.replace({0: np.nan})
        df = df[['inn'] + self.check_columns]
        df_funnel = df.rename(columns={'inn': 'Таргет'}).count().reset_index(
            name='count').rename(columns={'index': 'Уровни воронки'})
        df_funnel['sort_column'] = df_funnel['Уровни воронки'].map({'Таргет' : 2, 'Ошибки' : 1})
        df_funnel = df_funnel.sort_values(['sort_column', 'count'], ascending=False).drop('sort_column',
                                                                                          axis=1).reset_index(drop=True)
        df_funnel['Кол-во клиентов'] = 'Всего'
        check_columns = [x for x in list(list(df_funnel['Уровни воронки'].get_values())) if x in self.check_columns[1:]]
        df_funnel = df_funnel.append(
            pd.DataFrame(
                [['Таргет', df_funnel[df_funnel['Уровни воронки'] == 'Таргет']['count'].get_values()[0], 'Уникальных'],
                 ['Ошибки',
                  df_funnel[df_funnel['Уровни воронки'] == 'Ошибки']['count'].get_values()[0],
                  'Уникальных']]
                , columns=['Уровни воронки', 'count', 'Кол-во клиентов']), ignore_index=True, sort=False)
        for i in range(len(check_columns)):
            records_count = 0
            if i == 0:
                records_count += len(df[df[check_columns[i]] == 1]['inn'])
            else:
                records_count += len(df[df[check_columns[:i]].isna().all(axis='columns') & df[check_columns[i]] == 1]['inn'])
            df_funnel = df_funnel.append(pd.DataFrame([[check_columns[i], records_count, 'Уникальных']],
                                                      columns=['Уровни воронки', 'count', 'Кол-во клиентов']),
                                         ignore_index=True,
                                         sort=False)

        fig0 = px.funnel(df_funnel,
                         x='count',
                         y='Уровни воронки',
                         color='Кол-во клиентов',
                         title='Воронка автопроверок', opacity=0.9)
        fig0.show()

class RepositoryLoader(object):
    '''
    Класс для наполнения репозитория инсайтов

    Параметры
    ----------
    object: экземпляр класса cjm.GPConnector() для подключения к БД
    insight_repository: название таблицы репозитория инсайтов
    insight_repository_sbc: название таблицы итогового репозитория инсайтов, из которого инсайты прогружаются в SBC
    insight_repository_extra: название таблицы репозитория инсайтов с доп. атрибутами

    Атрибуты
    ----------
    gpconnector: экземпляр класса cjm.GPConnector() для подключения к БД

    Методы
    ----------
    load: метод для загрузки кампании в репозиторий
    delete: метод для удаления кампании из репозитория
    select: метод для вывода результатов загрузки в репозиторий на печать
    select_list: метод для вывода результатов загрузки в репозиторий в list
    select_df: метод для вывода результатов загрузки в репозиторий в датафрейм

    Таблицы
    ----------
    prom.insight_repository: репозиторий инсайтов insight_repository_sbc
    '''
    # TODO: 
    # варианты методов delte select для итогового репозитория sbc

    def __init__(self, gpconnector, repository_type: str = 'SBC'):
        self.gpconnector = gpconnector
        self.repository_type = repository_type
        if self.repository_type == 'SBC':
            self.insight_repository = "prom.insight_repository"
        elif self.repository_type == 'SAS':
            self.insight_repository = "prom.insight_repository_sas"
        self.insight_repository_sbc = "prom.insight_repository_sbc"  
        self.insight_repository_extra = "prom.insight_repository_extra"  
        self.errors = None
        self.correct = None

    def load(self, target, repository_type: str = 'SBC', reload: bool = False):
        '''
        Метод для загрузки кампании в репозиторий

        Параметры
        ----------
        target: sql-код запроса
            или название таблицы в формате схема.название
            или list
            или датафрейм;
            данные для загрузки в репозиторий
        repository_type: str, {'SBC', 'SAS'}, по умолчанию 'SBC'; указывает на целевой репозиторий (insight_repository/insight_repository_sas)
        reload: bool, по умаолчанию True; указывает требуется ли осуществить очистку репозитория от записей с таким request_id перед загрузкой инсайтов
        '''
        # TODO: 
        # подтягивание доп.атрибутов в insight_repository_extra
        # подтягивание kpp, cusomer_id, epk_id
        # нормализация инн

        @dispatch(pd.DataFrame)
        def get_request_id_dispatch(target):
            source_cd_lv2 = []
            request_id = []
            if 'source_cd_lv2' in target.columns:
                source_cd_lv2 = list(set(target['source_cd_lv2']))
            if 'request_id' in target.columns:
                source_cd_lv2 = list(set(target['request_id']))
            return source_cd_lv2 + request_id

        @dispatch(list)
        def get_request_id_dispatch(target):
            return list(set(np.array(target)[:,0]))

        @dispatch(str)
        def get_request_id_dispatch(target):
            source_cd_lv2 = []
            request_id = []
            if 'select' not in target.lower():
                if self.gpconnector.validate_target('select request_id from {source_name}'.format(source_name=target), check_function=False):
                    request_id = list(set(np.array(self.gpconnector.select_list('select request_id from {source_name}'.format(source_name=target)))[:,0]))
                if self.gpconnector.validate_target('select source_cd_lv2 from {source_name}'.format(source_name=target), check_function=False):
                    source_cd_lv2 = list(set(np.array(self.gpconnector.select_list('select source_cd_lv2 from {source_name}'.format(source_name=target)))[:,0]))
            else:
                if self.gpconnector.validate_target('select request_id from ({sql}) as t'.format(sql=target), check_function=False):
                    request_id = list(set(np.array(self.gpconnector.select_list('select request_id from ({sql}) as t'.format(sql=target)))[:,0]))
                if self.gpconnector.validate_target('select source_cd_lv2 from ({sql}) as t'.format(sql=target), check_function=False):
                    source_cd_lv2 = list(set(np.array(self.gpconnector.select_list('select source_cd_lv2 from ({sql}) as t'.format(sql=target)))[:,0]))                
            return source_cd_lv2 + request_id

        if self.repository_type == 'SBC':
            autocheck = Autocheck(self.gpconnector, target, columns='request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt')
            autocheck.check(check_attributes = True, limit = 100000)
            self.errors = autocheck.errors
            self.correct = autocheck.correct
            if len(self.errors) == 0:
                if reload:                    
                    request_ids = get_request_id_dispatch(target)
                    if request_ids:
                        request_ids = '{\"' + '", "'.join(list(map(str, request_ids))) + '\"}'
                        self.gpconnector.execute("delete from {insight_repository} where request_id = any('{request_ids}')"
                                                .format(insight_repository=self.insight_repository, request_ids = request_ids))
                self.gpconnector.insert(self.insight_repository, target, columns='request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt')
            else:
                autocheck.show_statistics()
                raise Exception("ERROR: Устраните указанные ошибки для загрузки таргета в репозиторий!")
        elif self.repository_type == 'SAS':
            autocheck = Autocheck(self.gpconnector, target, columns='source_cd_lv2 , inn , kpp , crm_id , epk_id , task_priority , task_type , task_km , product_id , offer_desc_pp , offer_desc , entity_type , offer_sum_val , offer_income_val , start_dt , end_dt , num_attr_01 , num_attr_02 , num_attr_03 , text_attr_01 , text_attr_02 , text_attr_03 , date_attr_01 , date_attr_02 , date_attr_03')
            autocheck.check(check_attributes = True, limit = 100000)
            self.errors = autocheck.errors
            self.correct = autocheck.correct
            if len(self.errors) == 0:
                if reload:
                    source_cd_lv2s = get_request_id_dispatch(target)
                    if source_cd_lv2s:
                        source_cd_lv2s = '{\"' + '", "'.join(list(map(str, source_cd_lv2s))) + '\"}'
                        self.gpconnector.execute("delete from {insight_repository} where source_cd_lv2 = any('{source_cd_lv2s}')"
                                                .format(insight_repository=self.insight_repository, source_cd_lv2s = source_cd_lv2s))
                self.gpconnector.insert(self.insight_repository, target, columns='source_cd_lv2 , inn , kpp , crm_id , epk_id , task_priority , task_type , task_km , product_id , offer_desc_pp , offer_desc , entity_type , offer_sum_val , offer_income_val , start_dt , end_dt , num_attr_01 , num_attr_02 , num_attr_03 , text_attr_01 , text_attr_02 , text_attr_03 , date_attr_01 , date_attr_02 , date_attr_03')
            else:
                autocheck.show_statistics()
                raise Exception("ERROR: Устраните указанные ошибки для загрузки таргета в репозиторий!")
        else:
            raise Exception("ERROR: Укажите нужный тип репозитория(SBC/SAS)!")

    def load_to_sbc(self, request_id):
        self.gpconnector.execute("delete from {insight_repository} where request_id = {request_id}"
                                 .format(insight_repository=self.insight_repository_sbc, request_id = request_id))
        self.gpconnector.insert(self.insight_repository_sbc, "select * from {insight_repository} where request_id = {request_id}"
                                .format(insight_repository=self.insight_repository, request_id = request_id)
                                , columns='insight_id, creation_dttm, request_id, scenario_id, inn, kpp, customer_id, epk_id, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt')

    def delete(self, request_id):
        '''
        Метод для удаления кампании из репозитория

        Параметры
        ----------
        request_id: int/str; идентификатор заявки request_id для SBC или source_cd_lv2 для SAS
        '''

        if self.repository_type == 'SBC':
            self.gpconnector.execute("delete from {insight_repository} where request_id = {request_id}"
                                    .format(insight_repository = self.insight_repository, request_id = request_id))
        elif self.repository_type == 'SAS':
            self.gpconnector.execute("delete from {insight_repository} where source_cd_lv2 = '{source_cd_lv2}'"
                                    .format(insight_repository = self.insight_repository, source_cd_lv2 = request_id))
        print("SUCCESS: Инсайты по заявке {request_id} удалены из репозитория".format(request_id = request_id))

    def select(self, request_id: int, limit: int = 100):
        '''
        Метод для вывода результатов загрузки в репозиторий на печать

        Параметры
        ----------
        request_id: str; идентификатор заявки
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        if self.repository_type == 'SBC':
            self.gpconnector.select("""select * from {insight_repository} where request_id = {request_id}"""
                                    .format(insight_repository = self.insight_repository, request_id = request_id), limit)
        elif self.repository_type == 'SAS':
            self.gpconnector.select("""select * from {insight_repository} where source_cd_lv2 = '{source_cd_lv2}'"""
                                    .format(insight_repository = self.insight_repository, source_cd_lv2 = request_id), limit)

    def select_list(self, request_id: int, limit: int = 100) -> list:
        '''
        Метод для вывода результатов загрузки в репозиторий в list

        Параметры
        ----------
        request_id: str; идентификатор заявки
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        if self.repository_type == 'SBC':
            return self.gpconnector.select_list(
                """select * from {insight_repository} where request_id = {request_id}"""
                .format(insight_repository = self.insight_repository, request_id = request_id), limit)
        elif self.repository_type == 'SAS':
            return self.gpconnector.select_list(
                """select * from {insight_repository} where source_cd_lv2 = '{source_cd_lv2}'"""
                .format(insight_repository = self.insight_repository, source_cd_lv2 = request_id), limit)

    def select_df(self, request_id: int, limit: int = 100) -> pd.DataFrame:
        '''
        Метод для вывода результатов загрузки в репозиторий в датафрейм

        Параметры
        ----------
        request_id: str; идентификатор заявки
        limit: int, по умолчанию 100; ограничение количества выводимых строк
        '''

        if self.repository_type == 'SBC':
            return self.gpconnector.select_df(
                """select * from {insight_repository} where request_id = {request_id}"""
                .format(insight_repository = self.insight_repository, request_id = request_id), limit)
        elif self.repository_type == 'SAS':
            return self.gpconnector.select_df(
                """select * from {insight_repository} where source_cd_lv2 = '{source_cd_lv2}'"""
                .format(insight_repository = self.insight_repository, source_cd_lv2 = request_id), limit)

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