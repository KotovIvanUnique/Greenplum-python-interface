from multipledispatch import dispatch
import pandas as pd
import plotly.express as px
import numpy as np

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
            self.check_attributes_column = """case when {check_attributes} = True and (t.request_id is null or t.scenario_id is null or t.inn is null or t.product_id is null or t.insight_desc is null or t.insight_sum_val is null or t.insight_income_val is null or t.insight_start_dt is null or t.insight_end_dt is null) then 1 end"""
            self.check_dates_column = """case when {check_attributes} = True and (t.insight_start_dt < current_date or t.insight_end_dt <= current_date) then 1 end"""

        @dispatch(pd.DataFrame)
        def check_dispatch(target):
            target_columns = ', '.join(list(target.columns))

            if 'product_id' in target_columns.lower():
                set_check_product_sql(self)
            if 'request_id' in target_columns.lower():
                set_check_segment_sql(self)
            if all(elem in target_columns.upper() for elem in ['request_id', 'scenario_id', 'inn', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']):
                set_check_attributes_sql(self)

        @dispatch(pd.DataFrame, str)
        def check_dispatch(target, columns):
            target_columns = ', '.join(list(target.columns))

            if 'product_id' in columns.lower() and 'product_id' in target_columns.lower():
                set_check_product_sql(self)
            if 'request_id' in columns.lower() and 'request_id' in target_columns.lower():
                set_check_segment_sql(self)
            if all(elem in columns.lower() for elem in ['request_id', 'scenario_id', 'inn', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']) and all(elem in target_columns.lower() for elem in ['inn']):
                set_check_attributes_sql(self)

        @dispatch(list, str)
        def check_dispatch(target, columns):
            if 'product_id' in columns.lower():
                set_check_product_sql(self)
            if 'request_id' in columns.lower():
                set_check_segment_sql(self)
            if all(elem in columns.lower() for elem in ['request_id', 'scenario_id', 'inn', 'product_id', 'insight_desc', 'insight_sum_val', 'insight_income_val', 'insight_start_dt', 'insight_end_dt']):
                set_check_attributes_sql(self)

        @dispatch(str)
        def check_dispatch(target):
            if 'select' not in target.lower():
                if self.gpconnector.validate_target('select product_id from {source_name}'.format(source_name=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from {source_name}'.format(source_name=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from {source_name}'.format(source_name=target), check_function=False):
                    set_check_attributes_sql(self)
            else:
                if self.gpconnector.validate_target('select product_id from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from ({sql}) as t'.format(sql=target), check_function=False):
                    set_check_attributes_sql(self)

        @dispatch(str, str)
        def check_dispatch(target, columns):
            if 'select' not in target.lower():
                if self.gpconnector.validate_target('select product_id from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from (select {columns} from {source_name}) as t'.format(columns=columns, source_name=target), check_function=False):
                    set_check_attributes_sql(self)
            else:
                if self.gpconnector.validate_target('select product_id from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
                    set_check_product_sql(self)
                if self.gpconnector.validate_target('select request_id from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
                    set_check_segment_sql(self)
                if self.gpconnector.validate_target('select request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt from (select {columns} from ({sql}) as t) as t'.format(columns=columns, sql=target), check_function=False):
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