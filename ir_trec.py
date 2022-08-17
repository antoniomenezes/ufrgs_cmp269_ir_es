#!/usr/bin/python
# coding=utf-8

# Exemplos de uso da biblioteca trec
# https://notebooks.githubusercontent.com/view/ipynb?azure_maps_enabled=false&browser=chrome&color_mode=auto&commit=3c0727b50104b5a38d94ecc935196ee70403eb39&device=unknown&enc_url=68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f69656c61622f656c61737469633449522f336330373237623530313034623561333864393465636339333531393665653730343033656233392f747265635f72756e2f456c61737469637365617263682532305452454325323052756e2e6970796e62&enterprise_enabled=false&logged_in=false&nwo=ielab%2Felastic4IR&path=trec_run%2FElasticsearch+TREC+Run.ipynb&platform=android&repository_id=97041336&repository_type=Repository&version=102

# pip install ir-kit
# https://github.com/hscells/ir-kit

# Downloading english language model
# python -m spacy download en_core_web_sm

# Downloading spanish language model
# python -m spacy download es_core_news_sm

from datetime import datetime
from html import entities
import os
from string import punctuation

from elasticsearch import Elasticsearch
from trec import run
import pandas as pd
from pandas import DataFrame, Series
import matplotlib.pyplot as plt

import nltk
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from nltk.tokenize import word_tokenize

import spacy
import numpy as np

from ir_utilidades import get_tags, get_topics_text, get_tag_value, printProgressBar

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

# Parametros de entrada

#path = os.getcwd()
path = os.getcwd().replace('\\', '/')

# para o índice gh95
#nome_do_indice = 'gh95'
#arquivo_de_topicos = 'topicos05.txt'
#fieldlist=['HEADLINE', 'TEXT']
#language = 'en'
#sw_list = stopwords.words('english')
#sw_list_extra = ['find','documents','relevant']
#sw_list = sw_list + sw_list_extra

# para o índice efe95
nome_do_indice = 'efe95'
arquivo_de_topicos = 'Topicos.txt'
fieldlist=['TITLE', 'TEXT']
language = 'es'
sw_list = stopwords.words('spanish')
sw_list_extra = ['encontrar','documentos','relevantes','información']
sw_list = sw_list + sw_list_extra



def query_all_words(topics_dir, topics_filename, index_name, output_dir, output_filename, fieldlist):
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

        i = 0
        for topic in topics:
            i = i + 1
            # obter os valores de tags para cada topico
            num = get_tag_value(topic, 'num')
            title = get_tag_value(topic, 'title')
            desc = get_tag_value(topic, 'desc')
            narr = get_tag_value(topic, 'narr')

            printProgressBar(i, len(topics), prefix = 'Executando query : topico '+str(num).strip(), suffix = 'Completo', length = 50)

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body={"query": {"multi_match": {"query" : query_text, "fields": fieldlist} } }, size=100)
            
            # Preparar linhas de resultados para o arquivo de saída
            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='antonio', topic=num))

            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()

def query_entities_no_stopwords(stopwords_list, topics_dir, topics_filename, index_name, output_dir, output_filename, language, fieldlist):
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

        i = 0
        for topic in topics:
            i = i + 1
            # obter os valores de tags para cada topico
            num = get_tag_value(topic, 'num')
            title = get_tag_value(topic, 'title')
            desc = get_tag_value(topic, 'desc')
            narr = get_tag_value(topic, 'narr')

            title_no_sw = remove_stopwords(title, stopwords_list)
            desc_no_sw = remove_stopwords(desc, stopwords_list)

            printProgressBar(i, len(topics), prefix = 'Executando query : topico '+str(num).strip(), suffix = 'Completo', length = 50)

            entities = list(set(extract_entities(title+' '+desc, language)))
            entities_text = ''
            if len(entities) > 0:
                entities_text = ' '.join(entities)

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body= {
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {
                                    "content": {
                                        "query": title_no_sw+' '+entities_text+' '+desc_no_sw,
                                        "boost": 10}
                                    }
                                }
                            ],
                            "must": [
                                {"match": {fieldlist[1] : title_no_sw+' '+entities_text } }                                
                            ]
                        }
                    }
                }, size=100
            )
            
            # Preparar linhas de resultados para o arquivo de saída
            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='antonio', topic=num))

            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()


def remove_stopwords(text, stopwords_list):
    # removing punctuation
    for ch in punctuation:
        text = text.replace(ch, ' ')
    text = text.replace('\n', ' ')
    new_text = ''
    for word in text.split(' '):
        if word.lower() not in stopwords_list:
            new_text = new_text + ' ' + word
    return new_text.strip()

def extract_entities(text, language):
    entities = []
    if language == 'en':
        nlp = spacy.load("en_core_web_sm")
    elif language == 'es':
        nlp = spacy.load("es_core_news_sm")
    else:
        raise Exception("Language not supported")
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in ['PERSON', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'WORK_OF_ART', 'LAW', 'LANGUAGE', 'DATE']:
            entities.append(ent.text.strip())
    return entities

def remove_avoided_sentences(text, language):
    avoided_expressions = []
    allowed_sents = []
    if language == 'en':
        avoided_expressions.append('not relevant')
        avoided_expressions.append('not pertinent')
        avoided_expressions.append('not of interest')
        nlp = spacy.load("en_core_web_sm")
    elif language == 'es':
        avoided_expressions.append('no son relevantes')
        avoided_expressions.append('no son de interés')
        avoided_expressions.append('no se considerarán relevantes')
        avoided_expressions.append('no se considerarán de interés')
        nlp = spacy.load("es_core_news_sm")
    else:
        raise Exception("Language not supported")

    # find sentences with the avoided expressions    
    doc = nlp(text)
    for sent in doc.sents:
        for expr in avoided_expressions:
            if not (expr.lower() in sent.text.lower()):
                allowed_sents.append(sent.text)
            #else:
            #    print('Sentence avoided: '+sent.text)
    
    # return the text without the avoided words
    return ' '.join(allowed_sents).strip()


def get_similar(word, nlp):
    token = nlp(word)[0]
    #print(token._.wordnet.synsets())
    #print(token._.wordnet.lemmas())
    #print(token._.wordnet.wordnet_domains())


def most_similar(word, nlp, topn=3):
    word = nlp.vocab[word]
    queries = [
        w for w in word.vocab 
        if w.is_lower == word.is_lower and w.prob >= -15 and np.count_nonzero(w.vector)
    ]
    by_similarity = sorted(queries, key=lambda w: word.similarity(w), reverse=True)
    return [(w.lower_,w.similarity(word)) for w in by_similarity[:topn+1] if w.lower_ != word.lower_]

def expanding_vocabulary(text, language):
    if language == 'en':
        nlp = spacy.load("en_core_web_sm")
    elif language == 'es':
        nlp = spacy.load("es_core_news_sm")
    else:
        raise Exception("Language not supported")

    nlp.add_pipe("spacy_wordnet", after='tagger', config={'lang': nlp.lang})

    new_text = text
    doc = nlp(text)
    for token in doc:
        if not token.is_stop:
            get_similar(token.text, nlp)
            similar = most_similar(token.text.lower(), nlp)
            for word, similarity in similar:
                if word not in text:
                    new_text = new_text + ' ' + word
    return new_text

"""
def expanding_vocabulary(text, language, topn=3):
    if language == 'en':
        nlp = spacy.load("en_core_web_sm")
    elif language == 'es':
        nlp = spacy.load("es_core_news_sm")
    doc = nlp(text)
    new_words = []
    for token in doc:
        if token.is_stop == False:
            word_v = nlp.vocab[str(token.text)]
            queries = [
                w for w in word_v.vocab 
                if w.is_lower == word_v.is_lower and w.prob >= -15 and np.count_nonzero(w.vector)
            ]
            by_similarity = sorted(queries, key=lambda w: word_v.similarity(w), reverse=True)
            new_words = new_words + [(w.lower_,w.similarity(word_v)) for w in by_similarity[:topn+1] if w.lower_ != word_v.lower_]
    print("new_words", new_words)
    return text+' '.join(new_words)    
"""

def query_word_phrase_stopwords(stopwords_list, topics_dir, topics_filename, index_name, output_dir, output_filename, language, fields):
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
        f_query = open(output_dir+'/'+output_filename.replace('.txt','')+'_queries.txt', 'w', encoding='utf-8')

        i = 0
        for topic in topics:
            i = i + 1
            # obter os valores de tags para cada topico
            num = get_tag_value(topic, 'num')
            title = get_tag_value(topic, 'title')
            desc = get_tag_value(topic, 'desc')
            narr = get_tag_value(topic, 'narr')
            narr = remove_stopwords(narr, sw_list_extra)
            title_no_sw = remove_stopwords(title, stopwords_list)
            desc_no_sw = remove_stopwords(desc, stopwords_list)
            narr_no_sw = remove_stopwords(narr, stopwords_list)

            printProgressBar(i, len(topics), prefix = 'Executando query : topico '+str(num).strip(), suffix = 'Completo', length = 50)

            #expanded_text = expanding_vocabulary(title_no_sw+' '+desc_no_sw, language)
            #print(title_no_sw+' '+desc_no_sw)
            #print(expanded_text)

            #avoided_text = avoid_words(remove_stopwords(narr, sw_list_extra), language, stopwords_list)

            #entities = list(set(extract_entities(title+' '+desc+' '+narr, language)))
            entities = list(set(extract_entities(title+' '+desc, language)))
            entities_text = ''
            if len(entities) > 0:
                entities_text = ' '.join(entities)

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body= {
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {fields[0] : title_no_sw } },
                                #{"match": {fields[1] : remove_stopwords(title+' '+desc+' '+narr, sw_list_extra)} },
                                #{"match": {fields[1] : title_no_sw+' '+desc_no_sw} }, #+' '+narr_no_sw} }, 
                                {"match": {fields[1] : title_no_sw + ' ' + desc_no_sw +' '+narr_no_sw} },                               
                                {"match_phrase": {fields[0]: title} },
                                {"match": {
                                    "content": {
                                        "query": title_no_sw+' '+entities_text,
                                        "boost": 3}
                                    }
                                }
                            ],
                            "must": [
                                {"match": {fields[1] : title_no_sw+' '+entities_text } }                                
                            ]
                        }
                    }
                }, size=100
            )

            query_string = """{"query": {
                        "bool": {
                            "should": [
                                {"match": {\""""+fields[0]+"""\" : \""""+title_no_sw+"""\" } },
                                {"match": {\""""+fields[1]+"""\" : \""""+title_no_sw + ' ' + desc_no_sw + ' '+narr_no_sw +"""\"} },                                
                                {"match_phrase": {\""""+fields[0]+"""\": \""""+title+"""\"} },
                                {"match": {
                                    "content": {
                                        "query": \""""+title_no_sw+' '+entities_text+"""\",
                                        "boost": 10}
                                    }
                                }
                            ],
                            "must": [
                                {"match": {\""""+fields[1]+"""\" : \""""+title_no_sw+' '+entities_text+"""\" } }
                            ]
                        }
                    }
                }"""
            f_query.write(query_string+'\n')	 

            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='antonio', topic=num))

            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()
        f_query.close()



#generate_results_using_all_words(path+'/cmp269/gh95', 'topicos05.txt', 'gh95', path+'/cmp269/gh95', 'saida_es.txt')
#generate_results_word_phrase_stopwords(sw_list, path+'/cmp269/gh95', 'topicos05.txt', 'gh95', path+'/cmp269/gh95', 'saida_es_2.txt')
#generate_results_no_punctuation(path+'/cmp269/gh95', 'topicos05.txt', 'gh95', path+'/cmp269/gh95', 'saida_es_2.txt')

print('Queries com Concatenacao de Todos os Textos')
query_all_words(path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_allwords_es.txt', fieldlist)

print('Queries com Entidades e Sem Stopwords')
query_entities_no_stopwords(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_entities_no_stopwords_es.txt', language, fieldlist)

print('Queries com Entidades, Sem Stopwords, Removendo instrucoes nao relevantes e com composicoes Bool')
query_word_phrase_stopwords(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_nostopwords_es.txt', language, fields=fieldlist)

print('Queries concluidas')

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