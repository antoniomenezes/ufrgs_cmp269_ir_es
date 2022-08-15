#!/usr/bin/python
# coding=utf-8

import re
import os
from ir_utilidades import sgml_to_json, sgml_to_ndjson

path = os.getcwd().replace('\\', '/')
nome_do_indice = 'efe95'

print('convertendo arquivos SGML para NDJSON...')
for filename in os.listdir(path+'/cmp269/'+nome_do_indice):
    if filename.endswith('.sgml'):
        #print('convertendo',filename,'de SGML para NDJSON')
        sgml_to_ndjson(nome_do_indice, path+'/cmp269/'+nome_do_indice, filename, path+'/cmp269/'+nome_do_indice+'_ndjson')

f = open(path+'/cmp269/'+nome_do_indice+'_ndjson/ingestao.bat', 'w', encoding='utf-8')

for filename in os.listdir(path+'/cmp269/'+nome_do_indice+'_ndjson'):
    if filename.endswith('.json'):
        comando = 'curl.exe -XPOST localhost:9200/'+nome_do_indice+'/_bulk?pretty --data-binary "@'+path+'/cmp269/'+nome_do_indice+'_ndjson/'+filename+'" -H \'Content-Type: application/json\' --output '+path+'/cmp269/'+nome_do_indice+'_ndjson/'+filename.replace('.json','.log')+'\n'
        print(comando)	
        f.write(comando+'\n')	

        #os.system(comando)
        #print('curl.exe -XPOST localhost:9200/gh95/_bulk?pretty --data-binary "@'+filename+'" -H \'Content-Type: application/json\' \n')
        #print('curl.exe -XPOST localhost:9200/gh95/_bulk?pretty --data-binary "@'+filename+'" -H \'Content-Type: application/json\' --output '+filename.replace('.json','.log')+' \n')

f.close()
print('')

for filename in os.listdir(path+'/cmp269/'+nome_do_indice+'_ndjson'):
    if filename.endswith('.log'):
        with open(path+'/cmp269/'+nome_do_indice+'_ndjson'+'/'+filename, 'r') as f:
            text = f.read()
            if (text.find('"errors" : true,') != -1):
                print('erro dectado em',filename)

'''
for filename in os.listdir(path+'/cmp269/'+nome_do_indice):
    if filename.endswith('.sgml'):
        print('convertendo',filename,'de SGML para JSON')
        sgml_to_json('gh95', path+'/cmp269/'+nome_do_indice, filename, path+'/cmp269/'+nome_do_indice+'_json')
'''
