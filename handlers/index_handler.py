import tornado.web


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        response = {
            'status': 'OK'
        }
        self.set_status(200)
        self.write(response)
        return
