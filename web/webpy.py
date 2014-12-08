# coding: utf-8

#import sys
#sys.path.append('/data/komoxo/venus')

import web
from web import form
render = web.template.render('templates/')

from venus.utils import usearch
global search
search = usearch.Search()

import datetime
import re
urls = (
    '/', 'index',
    '/query', 'query'
)

class index:
    def GET(self):
        return render.query()

class query:
    def POST(self):
        input = web.input()['data']
        if not input:
            raise web.seeother('/')
        start = datetime.datetime.now()
        srt, exqs, rawqs, hit, index_cost, delta_query = search.search(input, topn=5)
        if not srt:
            raise web.seeother('/')
        details = search.details(srt)
        end_detail = datetime.datetime.now()

        if not details:
            raise web.seeother('/')
        ret = []
        hit = sorted(hit, key=lambda x:len(x), reverse=True)
        for x in details:
            score = x[1]
            detail = "docId: " + x[0]['docid'] + ", Name: " + x[0]['apk_title'] + ", Version: " + x[0]['versionname'] + ", Tags:" + x[0]['tag']
            for i in hit:
                detail = re.sub(i, u'<strong style="color:#228B22">%s</strong>' %i, detail)
            brief = x[0]['brief']
            for q in exqs:
                brief = re.sub(q[0], u'<strong style="color:#ff0000">%s</strong>' %q[0], brief)
            ret.append([score, detail, brief])

        end_show = datetime.datetime.now()

        delta_detail = end_detail - start
        delta_detail = delta_detail.total_seconds()

        delta_show = end_show - start
        delta_show = delta_show.total_seconds()
        return render.result(ret, index_cost, delta_query, delta_detail, delta_show)

if __name__ == '__main__':
    app = web.application(urls, globals())
    app.run()
