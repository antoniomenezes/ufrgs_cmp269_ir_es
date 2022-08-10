#!/usr/bin/python
# coding=utf-8

from datetime import datetime
import os

from elasticsearch import Elasticsearch
from trec import run, qrels
import pandas as pd
from pandas import DataFrame, Series
import matplotlib.pyplot as plt

from ir_utilidades import get_tags, get_topics_text, get_tag_value

es = Elasticsearch(hosts=['http://localhost:9200'])

#<top>
#<num> 251 </num>
#<title> Alternative Medicine <title>
#<desc> Find documents discussing any kind of alternative or natural medical treatment including specific therapies such as acupuncture, homeopathy, chiropractics, or others. <desc>
#<narr> Relevant documents will provide general or specific information on the use of natural or alternative medical treatments or practices. <narr>
#</top>

#GET gh95/_search
#{
#  "from" : 0, "size" : 100,
#  "query": {
#    "multi_match": {
#        "query" : "Alternative Medicine"
#        , "fields": ["HEADLINE","TEXT"]
#    }
#  }
#}

def generate_results_using_all_words(topics_dir, topics_filename, index_name, output_dir, output_filename):
    with open(topics_dir+'/'+topics_filename, 'r') as f:
        # obter cada topico
        text = f.read()
        new_text = text
        topics = []
        next_topic = ''
        while(new_text.find('top>') != -1):
            next_topic = get_tag_value(new_text, 'top')            
            topics.append(next_topic)
            new_text = new_text.replace('<top>'+next_topic+'</top>', '')

        f = open(output_dir+'/'+output_filename, 'w', encoding='utf-8')

        for topic in topics:
            # obter os valores de tags para cada topico
            num = get_tag_value(topic, 'num')
            title = get_tag_value(topic, 'title')
            desc = get_tag_value(topic, 'desc')
            narr = get_tag_value(topic, 'narr')

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body={"query": {"multi_match": {"query" : query_text, "fields": ["HEADLINE","TEXT"]} } }, size=100)
            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='es', topic=num))

            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()

def generate_results_word_phrase_stopwords(topics_dir, topics_filename, index_name, output_dir, output_filename):
    with open(topics_dir+'/'+topics_filename, 'r') as f:
        # obter cada topico
        text = f.read()
        new_text = text
        topics = []
        next_topic = ''
        while(new_text.find('top>') != -1):
            next_topic = get_tag_value(new_text, 'top')            
            topics.append(next_topic)
            new_text = new_text.replace('<top>'+next_topic+'</top>', '')

        f = open(output_dir+'/'+output_filename, 'w', encoding='utf-8')

        for topic in topics:
            # obter os valores de tags para cada topico
            num = get_tag_value(topic, 'num')
            title = get_tag_value(topic, 'title')
            desc = get_tag_value(topic, 'desc')
            narr = get_tag_value(topic, 'narr')

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body= {"query": {
                "bool": {"should": 
                            [ 
                                { "match" : {"content" : {"query" : "Alternative Medicine", "operator" : "or"}} },
                                { "match" : {"content" : {"query" : "Alternative Medicine", "operator" : "and"}} },
                                { "match_phrase" : {"content" : {"query" : "Alternative Medicine", "boost" : 2}} } 
                            ] 
                        }
                    } 
                }, size=100)

            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='es', topic=num))

            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()



path = os.getcwd()

print(path)

generate_results_using_all_words(path+'/cmp269/gh95', 'topicos05.txt', 'gh95', path+'/cmp269/gh95', 'saida_es.txt')
#generate_results_word_phrase_stopwords(path+'/cmp269/gh95', 'topicos05.txt', 'gh95', path+'/cmp269/gh95', 'saida_es_2.txt')

'''
query_text = "Alternative Medicine Find documents discussing any kind of alternative or natural medical treatment including specific therapies such as acupuncture, homeopathy, chiropractics, or others. Relevant documents will provide general or specific information on the use of natural or alternative medical treatments or practices."

res = es.search(index="gh95", body={"query": {"multi_match": {"query" : query_text, "fields": ["HEADLINE","TEXT"]} } }, size=100)

#print(res['hits']['hits'][0]['_source']['DOCID'])

hits = []
for rank, hit in enumerate(res['hits']['hits'], 1):
    hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='es', topic=251))

# 251	Q0	GH950321-000003	1	51.328102	okapi
for hit in hits:
    print(hit.__str__())

#print(run.TrecEvalRuns(hits).dumps())
'''	