import sqlparse

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