import tornado.web

import json


class SearchHandler(tornado.web.RequestHandler):

    def post(self):
        response = ['teste1', 'teste2']
        response = json.dumps(response)
        self.set_header("Content-Type", 'application/json; charset="utf-8"')
        self.set_status(200)
        self.write(response)
        return
