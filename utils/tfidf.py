# coding: utf-8

import jieba
import codecs
import math
import json
from os.path import join as pjoin, dirname, abspath, isfile
import sys
from operator import itemgetter

DOCS = pjoin(dirname(dirname(abspath(__file__))), "src", "apps_info.json")
fstop_words = pjoin(dirname(dirname(abspath(__file__))), "src", "stopwords.txt")
stop_words = set([line.strip() for line in codecs.open(fstop_words, 'r', 'utf-8')])

def load_docs(docs_name):
    """
        load docs.
        docs: {doc1: { 'docid': did1, 'catename': cname1,...}, 
                doc2: {'docid': did2, 'catename': cname2, ...}, ...}
    """
    if not isfile(docs_name):
        print "Make sure DOCS file exists!"
        sys.exit(1)
    docs = {}
    with codecs.open(docs_name, 'r', 'utf-8') as fdc:
        for line in fdc:
            line = line.strip()
            _doc = json.loads(line)
            docs[_doc['docid']] = _doc
    return docs

def wfilter(segment):
    return filter(lambda x:x not in stop_words, segment)


class TfIdf:
    def __init__(self, docs_name=DOCS):
        self.docs = load_docs(docs_name)
        self.entries = {}
        self.terms = []

    def words_tf(self, doc):
        """
        get terms frequence of each doc, and normalization the frequence
        return:
            entry: {w1: {'tf': tf1}, w2:{'tf': tf2}, ... }
            reserve: terms that exclude words from stopwords.
        """
        #print "###", doc['brief']
        segment = list(jieba.cut(doc['brief']))
        #print "==>>", " ".join(segment)
        reserve = wfilter(segment)
        entry = {}
        for w in reserve: # get term frequence
            entry[w] = entry.get(w, {})
            entry[w]['tf'] = entry[w].get('tf', 0) + 1
    
        for x in entry:  #normalization
            entry[x]['tf'] = 1.0 * entry[x]['tf'] / len(reserve)
        
        return entry, reserve
    
    def terms_tfidf(self):
        """
        get tfidf and all doc terms
        """
        for docid in self.docs:
            entry, reserve = self.words_tf(self.docs[docid])
            self.entries[docid] = entry
            self.terms += reserve
    
        self.terms = set(self.terms)  
        for w in self.terms: # invert doc frequence
            for docid in self.entries:
                if w in self.entries[docid]:
                    self.entries[docid][w]['idf'] = self.entries[docid][w].get(w, 0) + 1
 
        for docid in self.entries:
            for w in self.entries[docid]:
                assert self.entries[docid][w].has_key('tf')
                assert self.entries[docid][w].has_key('idf')
                self.entries[docid][w]['idf'] = math.log(1.0 * len(self.docs) / self.entries[w]['idf'] + 0.01)
                self.entries[docid][w]['tfidf'] = self.entries[docid][w]['tf'] * self.entries[docid][w]['idf']
                
        return self.entries, self.terms

if __name__ == "__main__":
    td = TfIdf()
    entries, terms = td.terms_tfidf()    
    x = entries['57160']
