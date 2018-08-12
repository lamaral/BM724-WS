import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.wsgi
import tornado.log

from handlers.config_handler import ConfigHandler
from handlers.index_handler import IndexHandler
from handlers.search_handler import SearchHandler
from handlers.query_handler import QueryHandler


def create_web_server():
    # Roteamento para as diferentes URIs
    handlers = [
        (r"/", IndexHandler),
        (r"/search", SearchHandler),
        (r"/query", QueryHandler),
        (r"/static/(.*)", tornado.web.StaticFileHandler, {'path': 'static/'}),
    ]

    # Configurações da aplicação
    settings = dict(
        # template_path='templates/'
        # autoreload=True
    )

    return tornado.web.Application(handlers, **settings)


if __name__ == '__main__':
    # Cria a aplicacao
    web_app = create_web_server()

    ioloop = tornado.ioloop.IOLoop.instance()

    # Inicia servidor web
    web_app.listen(ConfigHandler.config["http"]["bind_port"], address=ConfigHandler.config["http"]["bind_address"])
    ioloop.start()
    tornado.log.access_log.error("Teste")
