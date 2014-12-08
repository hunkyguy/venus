# coding: utf-8

"""
word2vec  ==>  {Qa : [(wordA, scoreA), (wordB, scoreB)], Qb: [(wordX, scoreX), (wordY, scoreY),  ], ...}

Data:
    word2vec
    Index
    docs

Input:
    queryset (qs) ==> [worda, wordb, ...]

Result:
    [(docid, rankA), (docid, rankB), ...]
"""
from os.path import join as pjoin, dirname, isfile, abspath
import sys
import codecs
import textrank
import json
import jieba
import datetime
import cPickle as pickle
import math

W2V = pjoin(dirname(dirname(abspath(__file__))), "src", "zsimilar.txt")
DOCS = pjoin(dirname(dirname(abspath(__file__))), "src", "apps_info.json")
serialIndex = pjoin(dirname(dirname(abspath(__file__))), "src", "index.pickle")

stop_words = textrank.stop_words


def wfilter(segment):
    segment = [x.strip() for x in segment if x.strip()]
    return filter(lambda x:x not in stop_words, segment)

class Search:
    def __init__(self, w2v_name=W2V, docs_name=DOCS):
        self.word2vec = self.load_w2v(w2v_name)
        self.docs = self.load_docs(docs_name)
        self.Index = self.initIndex()
        if not isfile(serialIndex):
            pickle.dump(self.Index, open(serialIndex, 'w'))
    
    def load_w2v(self, w2v_name):
        if not isfile(w2v_name):
            print "Make sure word2vec model file exists!"
            #sys.exit(1)
            return None
        similar = {}
        with codecs.open(w2v_name, 'r', 'utf-8') as wv:
            for line in wv:
                line = line.strip()
                if line:
                    item = line.split()
                    word = item[0]
                    topn = [(x.split(',')) for x in item[1].split('||')] 
                    topn = [(x[0], float(x[1])) for x in topn]
                    similar[word] = topn
        return similar

    def load_docs(self, docs_name):
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
    
    def producer(self, doc):
        """
        In: doc: {'docid': docid, 'brief': brief, 'apk_title': apk_title, ...}
        #return retdoc: {'docid': docid, 'terms': [(kw1, score1), (kw2, score2), ...]}
        return retdoc: {'docid': docid, 'terms': [(kw1, freq1), (kw2, freq2), ...]}
        """
        if not doc.has_key('docid') and doc.has_key('brief'):
            print "doc must have 'docid' and 'brief'!"
            #sys.exit(1)
        ret = {}
        ret['docid'] = doc['docid']
        #seg = textrank.text_keywords(doc['brief'], limit=None)
        seg = textrank.text_keywords(doc['brief'], limit=16)
        ret['terms'] = seg
        return ret
    
    def term_freq(self, doc):
        if not doc.has_key('docid') and doc.has_key('brief'):
            print "doc must have 'docid' and 'brief'!"
            #sys.exit(1)
        ret = {}
        ret['docid'] = doc['docid']
        seg = list(jieba.cut(doc['brief']))
        reverse = wfilter(seg)
        ret['terms'] = {}
        for w in reverse:
            ret['terms'][w] = ret['terms'].get(w, 0) + 1
        return ret

    def initIndex(self):
        """
        ###Inverted index: ==> {Wa: [(docidA, dsorceA), (docidB, dscoreB), ], ...}
        Inverted index: ==> {Wa: {docidA: dsorceA, docidB: dscoreB, ...}}
        In:  doc: same as producer
        """
        start_index = datetime.datetime.now()
        if isfile(serialIndex): # loads Index from file.
            invert = pickle.load(open(serialIndex, 'r'))
            end_index = datetime.datetime.now()
            delta_index = end_index - start_index
            delta_index = delta_index.total_seconds()
            print "##### Loads index cost: %f" %delta_index
            return invert
        invert = {}
        temp_doc_freq = {}
        for docid in self.docs:
            retdoc = self.producer(self.docs[docid])
            #retdoc = self.term_freq(self.docs[docid])
            if not retdoc['terms']:
                continue
            terms_fq = retdoc['terms'] 
            for word in terms_fq:
                invert[word] = invert.get(word, {})
                invert[word][docid] = invert[word].get(docid, {})  # store doc weight, title, tag
                invert[word][docid]['score'] = 1  # weight of current doc
                invert[word][docid]['title'] = self.docs[docid]['apk_title']  # title
                invert[word][docid]['tag'] = self.docs[docid]['tag']  # tag

        end_index = datetime.datetime.now()
        delta_index = end_index - start_index
        delta_index = delta_index.total_seconds()
        print "##### init index time cost: %f" %delta_index
        return invert

    def addIndex(self, doc):
        """
           In: doc: {'docid': docid, 'apk_title': apk_tile, 'brief': brief, ...}
        """
        retdoc = self.producer(doc)
        if retdoc:
            docid = retdoc['docid']
            terms = retdoc['terms']
            for word in terms:
                self.Index[word] = self.Index.get(word, {})
                self.Index[word][docid] = 1 # weight of doc

    def extendQueryset(self, qs, topn=None):
        if not qs:
            print "query set is empty!"
            #sys.exit(1)
            return []
        if not self.word2vec:
            print "word2vec not found!"
            return [(q, 1.0) for q in set(qs) if q]

        extendQ = {}
        for q in qs:   # 查询词得分初始化
            if not q:
                continue
            extendQ[q] = 1.0 # 将得分初始化成 1.0
    
        for q in qs: # 计算扩展词得分
            c = self.word2vec.get(q, None) # 根据查询词，扩展查询集合
    
            if c:
                c = c[:topn]
                for key, value in  c:
                    extendQ[key] = extendQ.get(key, 0.0) + value # 如扩展词存在，累加cosine值
    
        sortExtendQ = sorted(extendQ.items(), key=lambda x:x[1], reverse=True)
        #return [x[0] for x in sortExtendQ]
        return sortExtendQ
    
    def _search(self, qs, topn, limit, extend):
        index_cost = datetime.timedelta()   # time costs of getting all invert index
        begin_index = datetime.datetime.now()

        raw_qs = [(q, 1.0) for q in set(qs) if q]
        Qs = [(q, 1.0) for q in set(qs) if q]
        if extend:
            Qs = self.extendQueryset(qs, topn)
        
        end_index = datetime.datetime.now()
        index_cost += (end_index - begin_index) # add time costs of query set extending

        result = {}
        
        hit = set()
        for q in Qs[:limit]:
            begin_index = datetime.datetime.now()
            c = self.Index.get(q[0], None) # 从索引中取得文档 docids和对应的权重
            end_index = datetime.datetime.now()
            index_cost += (end_index - begin_index)
            if c:
                for docid in c:
                    # title and tag
                    title = set(wfilter(list(jieba.cut(c[docid]['title']))))
                    tag = set(wfilter(list(jieba.cut(c[docid]['tag']))))

                    result[docid] = result.get(docid, {})
                    result[docid]['s1'] = result[docid].get('s1', 0.0)
                    result[docid]['s2'] = result[docid].get('s2', 0.0)
                    result[docid]['n'] = result[docid].get('n', 0) + 1
                    result[docid]['ts'] = result[docid].get('ts', 0.0)

                    if q[0] in title:
                        result[docid]['ts'] += 1.0
                        if q[0] in qs:
                            hit.add(q[0])
                    if q[0] in tag:
                        result[docid]['ts'] += 0.5
                        if q[0] in qs:
                            hit.add(q[0])
                    
                    if q[0] not in qs:
                        result[docid]['s2'] = result[docid].get('s2', 0.0) + q[1] * c[docid]['score']
                    else:
                        result[docid]['s1'] = result[docid].get('s1', 0.0) + q[1] * c[docid]['score']
                    
        if not result:
            return [], Qs, raw_qs, list(hit), index_cost
        
        #sortResult ==> {docA: rankA, docB: rankB, ... }
        _sortResult = {}
        for docid in result: # 最终得分： s2 / n + s1
            #_sortResult[docid] = result[docid].get('s2', 0) / result[docid].get('n', len(Qs)) + result[docid].get('s1', 0) 
            _sortResult[docid] = result[docid].get('s2', 0) / result[docid].get('n', len(Qs)) + result[docid].get('s1', 0) 
            _sortResult[docid] += result[docid]['ts']

        # normalization
        #max_score = max(_sortResult.values()) if _sortResult.values() else 1
        #for docid in _sortResult:
        #    _sortResult[docid] = _sortResult[docid] / max_score
    
        sortResult = sorted(_sortResult.items(), key=lambda e:e[1], reverse=True)

        return sortResult, Qs, raw_qs, list(hit), index_cost
    
    def search(self, qstr, topn=None, limit=None, extend=True):
        if not qstr:
            print "query must not be empty"
            #sys.exit(701)
            return

        start_query = datetime.datetime.now()
        pre_query = datetime.timedelta()
        begin_index = datetime.datetime.now()

        raw_qs = wfilter(jieba.cut_for_search(qstr))

        end_index = datetime.datetime.now()
        pre_query += (end_index - begin_index)

        sortResult, Qs, raw_qs, hit, index_cost = self._search(raw_qs, topn=topn, limit=limit, extend=extend)
        
        index_cost += pre_query     # add time costs of pre_query
        index_cost = index_cost.total_seconds()

        end_query = datetime.datetime.now()
        delta_query = end_query - start_query
        delta_query = delta_query.total_seconds()

        return sortResult, Qs, raw_qs, hit, index_cost, delta_query

    def details(self, sret):
        """
            details: [(docA, rankA), (docB, rankB), ...]
        """
        details = []
        for x in sret:
            details.append((self.docs[x[0]], x[1]))
        return details
    
    def display(self, details, qs, topn=15):
        for show in details[:topn]:
            print "===" * 10
            print "## Score: %f" %show[1]
            print ">> docid:", show[0]['docid'], 
            print ">> apk_title:", show[0]['apk_title'], 
            print ">> version:", show[0]['versionname']
            brief = show[0]['brief']
            import re
            for q in qs:
                newq = u'\033[1;31;40m' + q[0] + u'\033[0m'
                brief = re.sub(q[0], newq, brief)
            print "Brief: %s" %brief

