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
# python -m spacy download es_core_news_lg

from datetime import datetime
import encodings
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

import gensim.downloader
from gensim.models import Word2Vec
from gensim.models import KeyedVectors

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

# Gerando arquivos de stopwords de acordo com o idioma
f = open(path+'/'+nome_do_indice+'/stopwords_'+language+'.txt', 'w', encoding='utf-8')
for s in sw_list:
    f.write(s+'\n')	
f.close()

# Download em SBW-vectors-300-min5.txt em https://www.kaggle.com/datasets/rtatman/pretrained-word-vectors-for-spanish?resource=download
#w2v_modelo = KeyedVectors.load_word2vec_format(path+"/"+"SBW-vectors-300-min5.txt")
#print('word2vec loaded')


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
        #nlp = spacy.load("es_core_news_lg")
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
                                        "boost": 3}
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

# https://github.com/dccuchile/spanish-word-embeddings
# https://www.kaggle.com/datasets/rtatman/pretrained-word-vectors-for-spanish?resource=download
# https://fasttext.cc/docs/en/pretrained-vectors.html
def query_word_phrase_stopwords_w2v(stopwords_list, topics_dir, topics_filename, index_name, output_dir, output_filename, language, fields):
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

            new_desc_no_sw_list = []
            for w in desc_no_sw.split(" "):
                words_similar = []
                try:
                    words_similar = w2v_modelo.most_similar(w, topn=3)
                except:
                    pass
                
                word_similar = ""
                if len(words_similar) > 0:
                    iw = 0
                    while((word_similar=="") and (iw<2)):
                        if words_similar[iw][0].lower() != w.lower():
                            word_similar = words_similar[iw][0]
                        iw = iw + 1
                if word_similar != "" and w not in entities and word_similar not in entities:
                    new_desc_no_sw_list.append(word_similar)

            desc_no_sw = desc_no_sw + " " + ' '.join(new_desc_no_sw_list)

            # Montar o texto composto da query
            query_text = title+' '+desc+' '+narr
            # Executar a query
            res = es.search(index=index_name, body= {
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {fields[0] : title_no_sw } },
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
                                        "boost": 3}
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


# Contabilizar termos após a execução das consultas para re-ranqueamento

def query_word_phrase_stopwords_qe(stopwords_list, topics_dir, topics_filename, index_name, output_dir, output_filename, language, fields):
    entities_docs = dict()
    doc_entities = dict()
    query_entities = dict()
    topics_hits = dict()

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

            # Registrar termos da query
            q_terms = set()
            for t in entities:
                q_terms.add(t)

            query_entities[num] = q_terms

            # Executar a query
            res = es.search(index=index_name, body= {
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {fields[0] : title_no_sw } },
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
                                        "boost": 3}
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

            for rank, hit in enumerate(res['hits']['hits'], 1):
                d_terms = set()
                doc_id = hit['_source']['DOCID']
                doc_title_text = hit['_source']['TITLE']
                doc_text = hit['_source']['TEXT']
                for t in (doc_title_text+' '+doc_text).split(" "):
                    if t in (entities):
                        d_terms.add(t)
                        try:
                            entities_docs[t].add(doc_id)
                        except:
                            entities_docs[t] = set()
                            entities_docs[t].add(doc_id)
                
                doc_entities[doc_id] = d_terms

            hits = []
            for rank, hit in enumerate(res['hits']['hits'], 1):
                hits.append(run.TrecEvalRun(rank=rank, doc_id=hit['_source']['DOCID'], q=0, score=hit['_score'], run_id='antonio', topic=num))

            topics_hits[num] = hits
            # Escrever os resultados no arquivo de saída
            for hit in hits:
                f.write(hit.__str__()+'\n')	
        f.close()
        f_query.close()

    return topics_hits, entities_docs, doc_entities, query_entities 

def re_ranking(topics_hits, entities_docs, doc_entities, query_entities, output_dir, output_filename):
    new_topics_hits = dict()
    for topic in topics_hits.keys():
        topic_entities = []
        try:
            topic_entities = query_entities[topic]
        except:
            pass


        if (len(topic_entities) > 0):
        
            docs_topic_entities = set()
            for e in topic_entities:
                docs_for_entity = []
                try:
                    docs_for_entity = entities_docs[e]
                except:
                    pass

                for d in docs_for_entity:
                    docs_topic_entities.add(d)  
            
            if len(docs_topic_entities) > 0:
                # hits : ranking de um determinado topic
                hits = topics_hits[topic] 

                print(topic, 'hits #',len(hits))

                docs_in_hits_with_entities = [h.doc_id for h in hits if h.doc_id in docs_topic_entities]

                new_hits = topics_hits[topic]

                for index in range(len(new_hits)-1, 1, -1):
                    nh = new_hits[index]
                    nh_prev = new_hits[index-1]

                    #print(index, nh.doc_id, index-1, nh_prev.doc_id)
                    
                    if (nh.doc_id in docs_in_hits_with_entities) and (nh_prev.doc_id not in docs_in_hits_with_entities):
                        nh.score = nh_prev.score + 0.000001
                        new_hits.remove(nh)
                        new_hits.insert(index-1, nh)
                        print(topic, 'inserindo:', nh.doc_id, index-1)

                # Reescrevendo os números do ranking
                new_hits2 = []
                index_h2 = 1
                for h2 in new_hits:
                    h2.rank = index_h2
                    new_hits2.append(h2)
                    index_h2 = index_h2 + 1
                
                new_hits = new_hits2

                print(topic, 'new hits #',len(new_hits))
                print("\n")
                for h_index in range(len(hits)):
                    if hits[h_index].score != new_hits[h_index].score:                        
                        print(
                            hits[h_index].topic, hits[h_index].rank, hits[h_index].doc_id, hits[h_index].score,
                            new_hits[h_index].topic, new_hits[h_index].rank, new_hits[h_index].doc_id, new_hits[h_index].score
                        )
                print("\n")

                new_topics_hits[topic] = new_hits

    for topic in topics_hits.keys():
        topic_n = 0
        try:
            topic_n = len(new_topics_hits[topic])
        except:
            pass

        if (topic_n == 0):
            new_topics_hits[topic] = topics_hits[topic]

    # Reorganizando topicos
    new_topics_hits_ordered = dict()
    topic_keys = list(new_topics_hits.keys())
    topic_keys.sort()
    for topic in topic_keys:
        new_topics_hits_ordered[topic] = new_topics_hits[topic]
    new_topics_hits = new_topics_hits_ordered

    f = open(output_dir+'/'+output_filename, 'w', encoding='utf-8')
    for topic in new_topics_hits.keys():
        hits = new_topics_hits[topic]
        # Escrever os resultados no arquivo de saída
        for hit in hits:
            f.write(hit.__str__()+'\n')	
    f.close()

    return new_topics_hits

print('Queries com Concatenacao de Todos os Textos')
query_all_words(path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_allwords_es.txt', fieldlist)

print('Queries com Entidades e Sem Stopwords')
query_entities_no_stopwords(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_entities_no_stopwords_es.txt', language, fieldlist)

print('Queries com Entidades, Sem Stopwords e com composicoes Bool')
query_word_phrase_stopwords(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_nostopwords_es.txt', language, fields=fieldlist)

#print('Queries com Entidades, Sem Stopwords, com composicoes Bool e novo vocabulário com word2vec (similar words)')
#query_word_phrase_stopwords_w2v(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_nostopwords_w2v_es.txt', language, fields=fieldlist)

print('Queries com Entidades, Sem Stopwords, com composicoes Bool e QE')
topics_hits, entities_docs, doc_entities, query_entities = query_word_phrase_stopwords_qe(sw_list, path+'/'+nome_do_indice, arquivo_de_topicos, nome_do_indice, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_nostopwords_qe_es.txt', language, fields=fieldlist)

print('Queries concluidas')

topics_hits2 = re_ranking(topics_hits, entities_docs, doc_entities, query_entities, path+'/'+nome_do_indice, 'saida_'+nome_do_indice+'_nostopwords_re_ranking_es.txt')

#print('Entidades dos documentos')
#for d in doc_entities.keys():
#    print(d, len(doc_entities[d]))

#print('\nEntidades das consultas')
#for q in query_entities.keys():
#    print(q, query_entities[q])

#print('\nDocumentos que referenciam entidades')
#for e in entities_docs.keys():
#    print(e, entities_docs[e])



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