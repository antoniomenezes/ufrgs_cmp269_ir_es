#!/usr/bin/python
# coding=utf-8

import re
import os
from ir_utilidades import sgml_to_json, sgml_to_ndjson

path = os.getcwd()

print('convertendo arquivos SGML para NDJSON...')
for filename in os.listdir(path+'/cmp269/gh95'):
    if filename.endswith('.sgml'):
        #print('convertendo',filename,'de SGML para NDJSON')
        sgml_to_ndjson('gh95', path+'/cmp269/gh95', filename, path+'/cmp269/gh95_ndjson')

for filename in os.listdir(path+'/cmp269/gh95_ndjson'):
    if filename.endswith('.json'):
        #print('curl.exe -XPOST localhost:9200/gh95/_bulk?pretty --data-binary "@'+filename+'" -H \'Content-Type: application/json\' \n')
        print('curl.exe -XPOST localhost:9200/gh95/_bulk?pretty --data-binary "@'+filename+'" -H \'Content-Type: application/json\' --output '+filename.replace('.json','.log')+' \n')

print('')


for filename in os.listdir(path+'/cmp269/gh95_ndjson'):
    if filename.endswith('.log'):
        with open(path+'/cmp269/gh95_ndjson'+'/'+filename, 'r') as f:
            text = f.read()
            if (text.find('"errors" : true,') != -1):
                print('erro dectado em',filename)

'''
for filename in os.listdir(path+'/cmp269/gh95'):
    if filename.endswith('.sgml'):
        print('convertendo',filename,'de SGML para JSON')
        sgml_to_json('gh95', path+'/cmp269/gh95', filename, path+'/cmp269/gh95_json')
'''

#import ndjson

#ndjson.writer(open(path+'/cmp269/efe95_json/efe95_doc_1.json', 'w'))

#ndjson_file = ndjson.load(open(path+'/cmp269/efe95_json/efe19951231_new.json'))
