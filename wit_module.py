#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 29 11:36:17 2023

@author: wit
"""


from scapy.all import rdpcap
import pandas as pd
pd.options.mode.chained_assignment = None
import os
import tqdm


def pcap2pandas(file):
    packets_table = pd.DataFrame(
    columns = ['num',
        'protocol',
        'time_unix',
        'version',
        'ihl',
        'tos',
        'len',
        'id',
        'flags1',
        'flags1_value',
        'frag',
        'ttl',
        'proto',
        'chksum1',
        'src',
        'dst',
        'sport',
        'dport',
        'len2',
        'seq',
        'ack',
        'dataofs',
        'reserved',
        'flags2',
        'flags2_value',
        'window',
        'chksum2',
        'urgptr',
        ])
    no_packets = pd.DataFrame(
    columns = ['num',
        'time_unix',
        'name',
        'L',
        ])

    packets = rdpcap(file)
    i = 0
    for L1 in packets:
#            print('               {}                   '.format(i), end='\r')
#           print(i,L1.name)
            i +=1
            time = L1.time
            if L1.name == 'Ethernet': 
                L1 = L1.payload
#                print(i,L1.name)
            if L1.name == 'IP': 
                L2 = L1.payload
                #L3 = L2.payload
            else:
                no_packets.loc[ len(no_packets.index )] = [
                            i,
                            time,
                            L1.name,
                            1,
                            ]                           
                continue
            if L2.name == "TCP": 
                packets_table.loc[ len(packets_table.index )] = [
                            i,
                            'TCP',
                            time,
                            L1.version,
                            L1.ihl,
                            L1.tos,
                            L1.len,
                            L1.id,
                            str(L1.flags),
                            L1.flags.value,
                            L1.frag,
                            L1.ttl,
                            L1.proto,
                            L1.chksum,
                            L1.src,
                            L1.dst,
                            L2.sport,
                            L2.dport,
                            len(L2),
                            L2.seq,
                            L2.ack,
                            L2.dataofs,
                            L2.reserved,
                            L2.flags,
                            L2.flags.value,
                            L2.window,
                            L2.chksum,
                            L2.urgptr,
                            ]    
            elif L2.name == "UDP": 
                packets_table.loc[ len(packets_table.index )] = [
                            i,
                            'UDP',
                            time,
                            L1.version,
                            L1.ihl,
                            L1.tos,
                            L1.len,
                            L1.id,
                            str(L1.flags),
                            L1.flags.value,
                            L1.frag,
                            L1.ttl,
                            L1.proto,
                            L1.chksum,
                            L1.src,
                            L1.dst,
                            L2.sport,
                            L2.dport,
                            len(L2),
                            '', #L2.seq,
                            '', #L2.ack,
                            '', #L2.dataofs,
                            '', #L2.reserved,
                            '', #L2.flags,
                            '', #L2.flags.value,
                            '', #L2.window,
                            L2.chksum,
                            '', #L2.urgptr,
                            ]                
            else:
                no_packets.loc[ len(no_packets.index )] = [
                            i,
                            time,
                            L2.name,
                            2,
                            ]                           
#                continue           
    packets_table['time'] =  packets_table['time_unix']-packets_table['time_unix'][0]

    return packets_table,no_packets

def get_max_flow(data):
    # Выдиление максимального потока из дампа трафика 
    vcdst = data.dst.value_counts()
    mcdst = vcdst.index[vcdst == vcdst.values.max()][0] 
    # опредеделили IP с максимальным в-вом пакетов
    vcdport = data.dport[data.dst == mcdst].value_counts()
    mcdport = vcdport.index[vcdport == vcdport.values.max()][0] 
    # опредеделили Port  с максимальным в-вом пакетов по IP 
    ff01 = data[((data.src == mcdst) & (data.sport == mcdport)) | ((data.dst == mcdst) & (data.dport == mcdport))]
    # ольтфитровали поток
    ff01.reset_index(inplace = True)
    ff01 = ff01.drop('index', axis= 1 )
    ff01 = ff01.assign(flow_dir=1)
    ff01.loc[ff01.src == ff01.src[0] ,'flow_dir'] =0
    # добвили направление потока
    return ff01


def get_signs(ff,tt):
    # функция формирования признаков 
    # ff - DataFrame трафика 
    #tt - Временной промежуток
    # разбисаем(маркируем) поток на временные интервамы
    ff = ff.assign(time_marc= 0)
    
    r = int(ff.time.iloc[-1] // tt)+1 # к-во выборок по tt секунд
    
    for t in range(r):
        ff.loc[((ff.time >= tt*t)  & 
                (ff.time < tt*(t+1)  )), 'time_marc'] = t
        
    signs = pd.DataFrame(columns = ['float_min',
                                    'float_max',
                                    'float_mean',
                                    'float_std',
                                    'float_sum',
                                    
                                    'float_len_min',
                                    'float_len_max',
                                    'float_len_mean',
                                    'float_len_std',
                                    'float_len_sum', # Количество переданых байт
                                    'float_bsec', # Количество переданых байт в секунду
                                    'float_pct', # Количество пакетов
                                    'float_psec', # Количество пакетов в сек
                                
                                    'fiat_min',
                                    'fiat_max',
                                    'fiat_mean',
                                    'fiat_std',
                                    'fiat_sum',

                                    'fiat_len_min',
                                    'fiat_len_max',
                                    'fiat_len_mean',
                                    'fiat_len_std',
                                    'fiat_len_sum',
                                    'fiat_len_bsec',
                                    'fiat_pct',
                                    'fiat_psec',

                                    'biat_min',
                                    'biat_max',
                                    'biat_mean',
                                    'biat_std',
                                    'biat_sum',
                                    
                                    'biat_len_min',
                                    'biat_len_max',
                                    'biat_len_mean',
                                    'biat_len_std',
                                    'biat_len_sum',
                                    'biat_len_bsec',
                                    'biat_pct',
                                    'biat_psec',
                                  
                                    ])
        
    for kv  in range(r):    
        # исключаем длительный временной  промежуток
        if not  (ff['time_marc'] == kv).any(): continue
    
        ffloat = ff[ff.time_marc == kv]
        ffloat['time_diff'] = ffloat.time - ffloat.time.shift(1)
        
        ffiat = ff[(ff.time_marc == kv) & (ff.flow_dir == 0)]
        ffiat['time_diff'] = ffiat.time - ffiat.time.shift(1)
        ffiat.reset_index(inplace = True)
        ffiat = ffiat.drop('index', axis= 1 )
       
        fbiat = ff[(ff.time_marc == kv) & (ff.flow_dir == 1)]
        fbiat.reset_index(inplace = True)
        fbiat['time_diff'] = fbiat.time - fbiat.time.shift(1)
        fbiat = fbiat.drop('index', axis= 1 )
          
        signs.loc[ len(signs.index )] = [
            ffloat.time_diff.min(),	#	float_min
            ffloat.time_diff.max(),	#	float_max
            ffloat.time_diff.mean(),#	float_mean
            ffloat.time_diff.std(),	#	float_std
            ffloat.time_diff.sum(), 
            
            
            ffloat.len.min(),	#количество байт float_min
            ffloat.len.max(),	#	float_max
            ffloat.len.mean(),#	float_mean
            ffloat.len.std(),	#	float_std
            ffloat.len.sum(),	#	float_std
            ffloat.len.sum()/ffloat.time_diff.sum(),	#	float_std
            len(ffloat.index),
            len(ffloat.index)/ffloat.time_diff.sum(),

            ffiat.time_diff.min(),	#	fiat_min
            ffiat.time_diff.max(),	#	fiat_max
            ffiat.time_diff.mean(),	#	fiat_mean
            ffiat.time_diff.std(),	#	fiat_std
            ffiat.time_diff.sum(),	#	fiat_std
            
            ffiat.len.min(),	#	fiat_min
            ffiat.len.max(),	#	fiat_max
            ffiat.len.mean(),	#	fiat_mean
            ffiat.len.std(),	#	fiat_std
            ffiat.len.sum(),	#	fiat_std
            ffiat.len.sum()/ffiat.time_diff.sum(),	#	fiat_std
            len(ffiat.index),
            len(ffiat.index)/ffiat.time_diff.sum(),
            
            fbiat.time_diff.min(),	#	biat_min
            fbiat.time_diff.max(),	#	biat_max
            fbiat.time_diff.mean(),	#	biat_mean
            fbiat.time_diff.std(),	#	biat_std
            fbiat.time_diff.sum(),	#	biat_std

            fbiat.len.min(),	#	biat_min
            fbiat.len.max(),	#	biat_max
            fbiat.len.mean(),	#	biat_mean
            fbiat.len.std(),	#	biat_std
            fbiat.len.sum(),	#	biat_std
            fbiat.len.sum()/fbiat.time_diff.sum(),	#	biat_std
            len(fbiat.index),
            len(fbiat.index)/fbiat.time_diff.sum(),

            #str('ff.traffic[tt*t]'), # метка класса target            
            ]        
    return signs

def get_files(directory,extl):
    # список файлов в директории по расширениям
    filess=[]
    for root, dirs, files in os.walk(directory):
        for f in files:
            base, exto = os.path.splitext(f)
            for exti in extl:
               
                if exto == '.' + exti  : 
                    filess.append((root,base,exto)) 
            
    return filess



def arff2pd(file):
    tst  =  pd.read_csv(file)
    dataW =tst[tst.iloc[:,0].str.contains("@")==False]
    #Удалить шапку
    #получить шапку
    colW = tst[tst.iloc[:,0].str.contains("@ATTRIBUTE ")].iloc[:,0].str.extract('@ATTRIBUTE\s*(.+?) ')
    columns = colW.iloc[:,0].tolist()
    dataW.columns = columns
    dataN = dataW.reset_index(drop = True)
    return dataN
    


