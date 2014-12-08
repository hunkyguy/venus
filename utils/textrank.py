# coding: utf-8

from __future__ import unicode_literals
import codecs
import re
import json
from os.path import join as pjoin, dirname
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

fstop_words = pjoin(dirname(dirname(__file__)), "src", "stopwords.txt")
stop_words = set([line.strip() for line in codecs.open(fstop_words, 'r', 'utf-8')])

def _keywords(docs, window, max_iter, threshold, damp):
    if not docs:
        return []
    docs = [x.strip().lower() for x in docs if x.strip()]
    words = {}
    vertices = {}

    bow = []
    for word in docs:
        words[word] = words.get(word, set())
        vertices[word] = vertices.get(word, 1.0)
        bow.append(word)

        if len(bow) > window: 
            bow.pop(0)
        
        for i in bow:
            for j in bow:
                if j != i:
                    words[j].add(i)
                    words[i].add(j)

    for _ in range(max_iter):
        # PageRank: Sv_i = (1-d) + d * sum(Sv_j/Out_j) 
        m = {}
        diff = 0
        for k in words:
            m[k] = 1 - damp
            for j in words[k]:
                m[k] += damp*vertices[j]/ len(words[j]) if words[j] else 0.0
            diff = max(abs(m[k] - vertices[k]), diff)
        vertices = m
        if diff <= threshold:
            break
    
    result = sorted(vertices.items(), key=lambda x: x[1], reverse=True)
    return result

def segment(text):
    import jieba
    return [x.strip() for x in list(jieba.cut(text)) if x.strip()]

def _segment(text):
    if not text:
        return []
    import requests
    import urllib
    base_url = "http://192.168.0.254:5000"
    quote = {}
    quote['text'] = text
    quote = urllib.urlencode(quote)
    url = base_url + quote
    page = requests.get(url)
    if not page.content:
        print "Text Segment Error"
        sys.exit(400)
    seg = json.loads(page.content)
    seg = [x.strip() for x in seg if x.strip()]
    return seg

def words_keywords(docs, score=False, limit=7, window=10, max_iter=200, threshold=0.001, damp=0.85):
    docs = filter(lambda x: x not in stop_words and len(x) > 1, docs)
    result = _keywords(docs, window=window, max_iter=max_iter, threshold=threshold, damp=damp)
    if score:
        return result[:limit]
    return [x[0] for x in result[:limit]]
    
def text_keywords(text, score=False, limit=7, window=10, max_iter=200, threshold=0.001, damp=0.85):
    docs = segment(text)
    docs = filter(lambda x: x not in stop_words and len(x) > 1, docs)
    result = _keywords(docs, window=window, max_iter=max_iter, threshold=threshold, damp=damp)
    if score:
        return result[:limit]
    return [x[0] for x in result[:limit]]
    
def keywords(docs, limit=7, window=10, max_iter=200, threshold=0.001, damp=0.85):
    docs = filter(lambda x: x not in stop_words and len(x) > 1, docs)
    result = _keywords(docs, window=window, max_iter=max_iter, threshold=threshold, damp=damp)
    return [x[0] for x in result[:limit]]

