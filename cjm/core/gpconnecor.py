import psycopg2
import pandas as pd
from multipledispatch import dispatch

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