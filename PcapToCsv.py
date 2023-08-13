import os
from scapy.all import rdpcap
import csv

path_list = []
directory = ('')


def pcap2csv(file):
    packets = rdpcap(file)
    with open(f'{file}.csv', 'w', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['Num,time_unix,version,ihl,tos,'
                         'len,id,flags,flags.'
                         'value,frag,ttl,proto,chksum,'
                         'src,dst,sport,dport,seq,'
                         'ack,dataofs,reserved,flags,flags_value,'
                         'window,chksum,urgptr'])
        i = 0
        for L1 in packets:
            print(i, L1.name)
            while L1.name != "IP":
                L1 = L1.payload
            L2 = L1.payload
            L3 = L2.payload
            if L2.name == "TCP":
                writer.writerow([f'{i + 1},{L1.time},{L1.version},{L1.ihl},{L1.tos},{L1.len},{L1.id},{L1.flags},'
                                 f'{L1.flags.value},{L1.frag},{L1.ttl},{L1.proto},{L1.chksum},{L1.src},{L1.dst},'
                                 f'{L2.sport},{L2.dport},{L2.seq},{L2.ack},{L2.dataofs},{L2.reserved},{L2.flags},'
                                 f'{L2.flags.value},{L2.window},{L2.chksum},{L2.urgptr}'])
                i += 1


file_names = os.listdir(directory)
for n in file_names:
    if n.endswith(".pcap"):
        path = directory + n
        path_list.append(path)

for file_name in path_list:
    pcap2csv(file_name)
