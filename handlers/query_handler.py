import tornado.web

import json


class QueryHandler(tornado.web.RequestHandler):
    
    def prepare(self):
        if self.request.body:
            try:
                json_data = tornado.escape.json_decode(self.request.body)
                self.request.arguments.update(json_data)
            except ValueError:
                message = 'Unable to parse JSON.'
                self.send_error(400, message=message) # Bad Request

    def post(self):
        response = '[  {    "columns":[      {"text":"Time","type":"time"},      {"text":"Country","type":"string"},      {"text":"Number","type":"number"}    ],    "rows":[      [1234567,"SE",123],     [1234567,"DE",231],      [1234567,"US",321]    ],    "type":"table"  }]' 
#        response = json.dumps(response)
        self.set_header("Content-Type", 'application/json; charset="utf-8"')
        self.set_status(200)
        self.write(response)
        return
