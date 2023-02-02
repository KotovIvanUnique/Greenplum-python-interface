import pandas as pd
import numpy as np
from multipledispatch import dispatch
from cjm.core import Autocheck

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
            autocheck = Autocheck(self.gpconnector, target, columns='request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt')
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
                self.gpconnector.insert(self.insight_repository, target, columns='request_id, scenario_id, inn, product_id, insight_desc, insight_sum_val, insight_income_val, insight_start_dt, insight_end_dt')
            else:
                autocheck.show_statistics()
                raise Exception("ERROR: Устраните указанные ошибки для загрузки таргета в репозиторий!")
        elif self.repository_type == 'SAS':
            autocheck = Autocheck(self.gpconnector, target, columns='source_cd_lv2 , inn , task_priority , task_type , task_km , product_id , offer_desc_pp , offer_desc , entity_type , offer_sum_val , offer_income_val , start_dt , end_dt , num_attr_01 , num_attr_02 , num_attr_03 , text_attr_01 , text_attr_02 , text_attr_03 , date_attr_01 , date_attr_02 , date_attr_03')
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
                self.gpconnector.insert(self.insight_repository, target, columns='source_cd_lv2 , inn , task_priority , task_type , task_km , product_id , offer_desc_pp , offer_desc , entity_type , offer_sum_val , offer_income_val , start_dt , end_dt , num_attr_01 , num_attr_02 , num_attr_03 , text_attr_01 , text_attr_02 , text_attr_03 , date_attr_01 , date_attr_02 , date_attr_03')
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