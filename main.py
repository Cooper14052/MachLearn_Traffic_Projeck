from module import *
import pandas as pd
import csv

pd.set_option('display.max_rows', None)  # Сброс ограничений на число столбцов
pd.set_option('display.max_columns', None)  # Сброс ограничений на число столбцов
pd.set_option('display.max_colwidth', None)  # Сброс ограничений на количество символов в записи


split_traffic('vpn_aim_chat1a.xlsx', 'Worksheet')
