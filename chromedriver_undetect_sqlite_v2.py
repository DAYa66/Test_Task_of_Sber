
import pandas as pd
from functools import lru_cache
from avito_parser import Avito_Parser
import sqlite3



@lru_cache(maxsize=None)
def load_previous_df(con):
    try:
        previous_df = pd.read_sql("SELECT * FROM output", con)
        print(f"previous_df: {previous_df}")
        return previous_df
    except Exception as error:
        print(f"Ошибка: {error}")

        try:
            create_table_query = '''CREATE TABLE IF NOT EXISTS output (
                                    id INTEGER PRIMARY KEY,
                                    ann_number TEXT NOT NULL,
                                    ann_title TEXT,
                                    ann_price TEXT,
                                    ann_address TEXT,
                                    ann_description TEXT,
                                    ann_date TEXT,
                                    ann_views TEXT NOT NULL,
                                    ann_url TEXT NOT NULL,
                                    key_word TEXT NOT NULL,
                                    parse_date timestamp
                                    update_date timestamp);'''

            cursor = con.cursor()
            cursor.execute(create_table_query)
            cursor.close()

            previous_df = pd.read_sql("SELECT * FROM output", con)
            print(f"previous_df: {previous_df}")

            return previous_df
        except sqlite3.Error as error:
            print("Ошибка при создании таблицы: ", error)

def make_out_df(previous_df: pd.DataFrame, parse_df: pd.DataFrame): # TODO: разбить функцию на мелкие функции по принципу единой ответственности
    previous_df_set = set(previous_df['ann_number'])
    parse_df_set = set(parse_df['ann_number'])
    intersect = list(parse_df_set.intersection(previous_df_set))
    print(f"intersect: {intersect}")

    # формирую выборку для выявления изменений в пересекающихся объявлениях из parse_df
    parse_df_compare = parse_df.query('ann_number in @intersect')[['ann_number', 'ann_price', 'ann_title',
                                                                   'ann_description']]

    # формирую выборку для выявления изменений в пересекающихся объявлениях из df
    previous_df_compare = previous_df.query('ann_number in @intersect')[['ann_number', 'ann_price',
                                                                         'ann_title', 'ann_description']]

    equal_list = []
    not_equal_list = []

    for i in range(len(intersect)):
        if previous_df_compare.loc[previous_df_compare['ann_number'] == i]\
                .equals(parse_df_compare.loc[parse_df_compare['ann_number'] == i]):
            equal_list.append(i)
        else:
            not_equal_list.append(i)
            parse_df.loc[parse_df['ann_number'] == i]['parse_date'] = previous_df.loc[previous_df['ann_number']\
                                                                                       == i]['parse_date']
            # вернул новому изменившемуся объявлению старую дату парсинга

    # удаляю из previous_df объявления, в которых изменились данные в соответствующих столбцах в parse_df
    index_not_equal_ann = list(previous_df.query('ann_number in @not_equal_list').index)
    previous_df.drop(index_not_equal_ann, inplace=True)

    # удаляю из parse_df объявления, в которых в пересекающихся объявлениях данные в соответствующих столбцах остались неизменными
    index_equal_ann = list(previous_df.query('ann_number in @equal_list').index)
    parse_df.drop(index_equal_ann, inplace=True)
    # формирую датасет, в котором объявления, уже имеющиеся в кеше, при изменении Цены, Названия, Описания обновляются в кеше и изменяется дата обновления на текущую. А новые объявления просто сохраняются
    out_df = pd.concat([previous_df, parse_df])

    return out_df

if __name__ == "__main__":
    con = sqlite3.connect("./db/avito_parser.db",
                            detect_types=sqlite3.PARSE_DECLTYPES |
                            sqlite3.PARSE_COLNAMES)
    parser = Avito_Parser(
        key_word = 'котики',
        version_chrome = 113  # 113.0.5672.126
    )
    parse_df = parser.parse()
    print(f"parse_df: {parse_df}")
    print(parser.parse.cache_info())

    previous_df = load_previous_df(con)
    print(load_previous_df.cache_info())

    out_df = make_out_df(previous_df, parse_df)

    out_df.to_excel("output.xlsx", index = False, sheet_name = 'avito_parse')
    out_df.to_sql("output", con=con, if_exists="replace", index=False)


