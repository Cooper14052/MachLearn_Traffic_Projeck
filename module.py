#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import pandas as pd
import math


def create_file():
    """Метод создаёт файл для результатов."""
    with open('res.csv', 'a', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['NAME_PROT',
                         'TIME_PERIOD',
                         'FLOWIAT_MAX',
                         'FLOWIAT_MIN',
                         'FLOWIAT_AVE',
                         'BIAT_MAX',
                         'BIAT_MIN',
                         'BIAT_AVE',
                         'FIAT_MAX',
                         'FIAT_MIN',
                         'FIAT_AVE',
                         'FB_PER_SEC',
                         'FP_PER_SEC'
                         ])
        f.close()

def add_to_file(file_name, period, biat_res,fiat_res, flowiat):
    try:
        with open('res.csv', 'a', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow([file_name.replace('csv', ''),period,biat_res[0],biat_res[1],biat_res[2],fiat_res[0],fiat_res[1],fiat_res[2], flowiat[0],flowiat[1],flowiat[2]])
    except:
        pass

def len_per_sec(len_list, time_list):
    """Метод считает кол-во байт в секунду"""
    sum_len = sum(len_list)
    len_psec_res = sum_len / time_list[-1]
    return len_psec_res


def pac_per_sec(read_file, time_list):
    """Метод считает кол-во пакетов в секунду"""
    rows = len(read_file)
    rows_float = float(rows)
    rows_res = rows_float / time_list[-1]
    return rows_res

def biat(src_list, time_list):
    try:
        biat_res_list = []
        biat_time_range = []
        biat_idx_list = []
        x = 0
        n = 0
        element0 = 0
        element1 = 1
        for idx in src_list:
            if idx == '205.188.12.91':
                biat_idx_list.append(x)
            x +=1
        for u in range(1,len(biat_idx_list) + 1):
            biat_time_range.append(time_list[biat_idx_list[n]])
            n += 1
        for g in range(0, len(biat_time_range) - 1):
            time_element = (biat_time_range[element1] - biat_time_range[element0])
            biat_res_list.append(time_element)
            element0 += 1
            element1 += 1
        BIAT_MAX = max(biat_res_list)
        BIAT_MIN = min(biat_res_list)
        BIAT_AVE = sum(biat_res_list)/(len(biat_res_list) + 1)
        return BIAT_MAX, BIAT_MIN, BIAT_AVE
    except:
        print('')

def fiat(src_list, time_list):
    try:
        fiat_res_list = []
        fiat_time_range = []
        fiat_idx_list = []
        x = 0
        n = 0
        element0 = 0
        element1 = 1
        for idx in src_list:
            if idx == '10.8.8.178':
                fiat_idx_list.append(x)
            x +=1
        for u in range(1,len(fiat_idx_list) + 1):
            fiat_time_range.append(time_list[fiat_idx_list[n]])
            n += 1
        for g in range(0, len(fiat_time_range) - 1):
            time_element = (fiat_time_range[element1] - fiat_time_range[element0])
            fiat_res_list.append(time_element)
            element0 += 1
            element1 += 1
        FIAT_MAX = max(fiat_res_list)
        FIAT_MIN = min(fiat_res_list)
        FIAT_AVE = sum(fiat_res_list)/(len(fiat_res_list) + 1)
        return FIAT_MAX, FIAT_MIN, FIAT_AVE
    except:
        print('')

def flowait(time_list):
    try:
        flowait_el_list = []
        element0 = 0  # Элемент списка первый
        element1 = 1  # Элемент списка последующий
        for l in range(len(time_list)-2):
            time_elem = time_list[element1] - time_list[element0]
            flowait_el_list.append(time_elem)
            element0 += 1
            element1 += 1
        FIATIAT_MAX = max(flowait_el_list)  # Расчет максимального значения списка
        FIATIAT_MIN = min(flowait_el_list)  # Расчет минимального значения списка
        FIATIAT_AVERAGE = sum(flowait_el_list) / (len(flowait_el_list) + 1)  # Расчет среднего арифметического списка
        return FIATIAT_MAX, FIATIAT_MIN, FIATIAT_AVERAGE
    except:
        print('')



def split_traffic(f_name):
    """Функция деления на потоки по 15 секунд."""
    file_name = f_name

    read_file = pd.read_csv(file_name, sep=',')
    range_count = len(read_file)

    before_time = 0  # С данного значения времени начинается поток
    then_time = 15  # До этого значения времени фильтрауется поток
    while True:

        next_time_unix = 0
        n1 = 0
        time_list = []
        src_list = []
        len_list = []

        for count in range(n1, range_count):
            if read_file.time_unix[next_time_unix] - read_file.time_unix[0] <= then_time and read_file.time_unix[next_time_unix] - read_file.time_unix[0] >= before_time and read_file.src[next_time_unix] in ('205.188.12.91', '10.8.8.178'):
                #print(read_file.Num[next_time_unix], end=' ')
                time = read_file.time_unix[next_time_unix] - read_file.time_unix[0]
                #print('[time]', time, end=' ')
                time_list.append(time)
                src = read_file.src[next_time_unix]
                #print('[src]', src, end=' ')
                src_list.append(src)
                #print('[sport]', read_file.sport[next_time_unix], end=' ')
                #print('[dst]', read_file.dst[next_time_unix], end=' ')
                #print('[dport]', read_file.dport[next_time_unix], end=' ')
                len_pac = read_file.len[next_time_unix]
                #print('[len]', len_pac, end='\n')
                len_list.append(len_pac)

            next_time_unix += 1
        if len(src_list) == 0:
            break
        period = f'{before_time}-{then_time}'
        biat_res = biat(src_list, time_list)
        fiat_res = fiat(src_list, time_list)
        flowiat = flowait(time_list)
        len_ps = len_per_sec(len_list, time_list)
        pac_ps = pac_per_sec(read_file, time_list)
        print(file_name.replace('.csv', ''), period, '[biat]',biat_res,'[fiat]',fiat_res, '[flowiat]',flowiat, '[len_ps]',len_ps, '[pac_ps]',pac_ps)
        add_to_file(file_name, period, biat_res,fiat_res, flowiat)

        before_time += 15
        then_time += 15
        n1 += len(time_list) + 1
        print('-' * 100)
