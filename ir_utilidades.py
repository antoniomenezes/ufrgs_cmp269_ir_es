#!/usr/bin/python
# coding=utf-8

import re
import os

def get_tags(text):
    """
    Recebe um texto e descobre a lista de tags SGML (<TAG> e </TAG>) contidas nesse texto
    """
    t = re.compile(r'</.*>', re.IGNORECASE)
    tags = t.findall(text)
    tags = [tag[2:-1] for tag in tags]
    tags = list(set(tags))
    return tags

def get_tagged_text(text):
    """
    Recebe um texto e retorna um dicionário com todas as tags e seus valores associados
    """
    tagged_values = {}
    tags = get_tags(text)
    tags_and_positions = {}
    tags_positions = {}
    for tag in tags:
        tags_positions[text.index('<'+tag+'>')] = '<'+tag+'>'
        #tags_positions[text.index('</'+tag+'>')] = '</'+tag+'>'
        tags_and_positions['<' + tag + '>'] = text.index('<' + tag + '>')
        tags_and_positions['</' + tag + '>'] = text.index('</' + tag + '>')
    tags_positions = sorted(tags_positions.items())

    for k in tags_positions:
        tagged_values[k[1].replace('<','').replace('>','')] = text[k[0]: tags_and_positions[ k[1].replace('<','</') ]].replace(k[1],'')
    return tagged_values

def get_doc_id(text):
    return text[text.find('<DOCID>')+7:text.find('</DOCID>')]

def get_docs_text(text):
    result = {}
    docs_texts = {}
    docs_positions = []
    last_text = text
    while last_text.find('<DOC>') != -1:
        docs_positions.append(last_text.find('<DOC>'))
        last_text = last_text[last_text.find('<DOC>')+5:] 
    
    for i in range(len(docs_positions)-1):
        docs_texts[i] = text[docs_positions[i]:docs_positions[i+1]-1].replace('</DOC>','').replace('<DOC>','')
    docs_texts[i+1] = text[docs_positions[i+1]+5:].replace('</DOC>','').replace('<DOC>','')

    for k in docs_texts.keys():
        result[get_doc_id(docs_texts[k])] = docs_texts[k]
    return result

def get_topic_id(text):
    return text[text.find('<num>')+5:text.find('</num>')]    

def get_topics_text(text):
    result = {}
    topics_texts = {}
    topics_positions = []
    last_text = text
    while last_text.find('<top>') != -1:
        topics_positions.append(last_text.find('<top>'))
        last_text = last_text[last_text.find('<top>')+5:] 
    
    for i in range(len(topics_positions)-1):
        topics_texts[i] = text[topics_positions[i]:topics_positions[i+1]-1].replace('</top>','').replace('<top>','')
    topics_texts[i+1] = text[topics_positions[i+1]+5:].replace('</top>','').replace('<top>','')

    for k in topics_texts.keys():
        result[get_topic_id(topics_texts[k])] = topics_texts[k]
    return result

def get_tag_value(text, tag):
    """
    Recebe um texto e um tag e retorna o valor associado a tag
    """
    indice_1 = text.index('<'+tag+'>')
    indice_2 = text.index('</'+tag+'>')
    return text[indice_1+len(tag)+2:indice_2]


def sgml_to_json(catalog_name, input_dir, filename, output_dir):
    """
    Recebe um arquivo SGML e salva cada DOC como um arquivo JSON
    """
    with open(input_dir+'/'+filename, 'r') as f:
        text = f.read()

    tags = list(set(get_tags(text)))

    text = text.replace('\t',' ').replace('\r',' ').replace('\n',' ')
    text = text.replace('<DOC>','POST '+catalog_name+'/_doc\n{').replace('</DOC>','}\n')
    text = text.replace('"','\\"')

    for tag in tags:
        text = text.replace('<'+tag+'>', '"'+tag+'":"')
        text = text.replace('</'+tag+'>', '",\n')

    text = text.replace(',\n}', '}\n')
    text = text.replace(',\n }', '}\n')
    #text = text.replace('/','\\/')    

    new_filename = filename.replace('.sgml','') + '.json'
    json_text = text
    with open(output_dir+'/'+new_filename, 'w', encoding='utf-8') as f:
        f.write(json_text)


def sgml_to_ndjson(catalog_name, input_dir, filename, output_dir):
    """
    Recebe um arquivo SGML e salva cada DOC como um arquivo JSON
    """
    with open(input_dir+'/'+filename, 'r') as f:
        text = f.read()

    tags = list(set(get_tags(text)))

    text = text.replace('\t',' ').replace('\r',' ').replace('\n',' ')
    text = text.replace('<DOC>','{ "index":{"_index":"'+catalog_name+'"} }\n{')
    text = text.replace('</DOC>','}\n')
    text = text.replace('\\','\\\\')
    text = text.replace('"','\\"')

    for tag in tags:
        text = text.replace('<'+tag+'>', '"'+tag+'":"')
        text = text.replace('</'+tag+'>', '",')

    text = text.replace(' {', '{')
    text = text.replace(', }', '}')
    #text = text.replace(',\n }', '}\n')
    #text = text.replace('/','\\/')    
    text = text.replace('{ \\"index\\":{\\"_index\\":\\"'+catalog_name+'\\"} }', '{ "index":{"_index":"'+catalog_name+'"} }')
    text = text + "\n"

    new_filename = filename.replace('.sgml','') + '.json'
    json_text = text
    with open(output_dir+'/'+new_filename, 'w', encoding='utf-8') as f:
        f.write(json_text)

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
