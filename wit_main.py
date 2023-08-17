#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 08:06:33 2023

@author: wit
"""
from wit_module import *


directory = '/home/wit/TMP/TestYastr/Traffic/NonVPN-PCAPs-01/'
#directory = '/home/wit/TMP/TestYastr/Traffic/VPN-PCAPS-01/'
#directory = '/home/wit/Laboratoria/Ястремской/AnaliseFlow/'

extl = ['pcap','pcapng']
folder = directory+'CSV'
if not os.path.isdir(folder):
     os.mkdir(folder)
folder = directory+'/CSV/'+'Signs_flow'
if not os.path.isdir(folder):
     os.mkdir(folder)

files  = get_files(directory,extl)
for root,file,ext in files: 
    print(file)
    pd_traff,no_packets = pcap2pandas(root+file+ext)
    print('pd_traff:        ',len(pd_traff ))
    print('no_packets: ',len(no_packets.index ))
    pd_traff.to_csv(root+'/CSV/'+file+'.csv', sep=';' )
    no_packets.to_csv(root+'/CSV/'+file+'NO'+'.csv', sep=';' )
    print(1)
    max_flow = get_max_flow(pd_traff)
    print(2)
    signs_flow = get_signs(max_flow,15)
    signs_flow.pritocol = pd_traff.protocol[0]
    signs_flow.traff = file
    signs_flow.to_csv(root+'/CSV/'+'Signs_flow/'+file+'.csv', sep=';' )
    print(3)

