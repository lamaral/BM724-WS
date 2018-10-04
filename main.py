import logging
import time
import signal

import tornado.httpserver
import tornado.ioloop
import tornado.web
import tornado.wsgi
import tornado.concurrent
import tornado.options
import tornado.log

from functools import partial

from handlers.config_handler import ConfigHandler
from handlers.index_handler import IndexHandler
from handlers.lastheard_handler import LastHeardHandler

from pubsub import MosquittoClient

MAX_WAIT_SECONDS_BEFORE_SHUTDOWN = 3


# Graceful shutdown
def sig_handler(server, mqtt, sig, frame):
    io_loop = tornado.ioloop.IOLoop.instance()

    def shutdown():
        tornado.log.app_log.info('Stopping MQTT client')
        mqtt.stop()
        tornado.log.app_log.info('Stopping http server')
        server.stop()
        tornado.log.app_log.info('Will shutdown in %s seconds ...',
                     MAX_WAIT_SECONDS_BEFORE_SHUTDOWN)
        io_loop.add_timeout(time.time() + MAX_WAIT_SECONDS_BEFORE_SHUTDOWN, io_loop.stop)

    tornado.log.app_log.warning('Caught signal: %s', sig)
    io_loop.add_callback_from_signal(shutdown)

    
def create_web_server():
    # Roteamento para as diferentes URIs
    handlers = [
        (r"/", IndexHandler),
        (r"/lh", LastHeardHandler),
        (r"/static/(.*)", tornado.web.StaticFileHandler, {'path': 'static/'}),
    ]

    # Configurações da aplicação
    settings = dict(
        autoreload=True
    )

    return tornado.web.Application(handlers, **settings)


if __name__ == '__main__':
    tornado.options.parse_command_line()

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s - %(name)-15.15s - %(levelname)-7.7s - %(message)s'))
    logger = logging.getLogger()
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False

    # Cria a aplicacao
    web_app = create_web_server()

    # Cria cliente MQTT
    web_app.mqtt_client = MosquittoClient(host=ConfigHandler.config["mqtt"]["host"], clean_session=True)
    web_app.mqtt_client.start()

    # Inicia servidor web
    server = tornado.httpserver.HTTPServer(web_app)
    server.listen(ConfigHandler.config["http"]["bind_port"], address=ConfigHandler.config["http"]["bind_address"])

    # Adiciona handlers de sinal
    signal.signal(signal.SIGTERM, partial(sig_handler, server, web_app.mqtt_client))
    signal.signal(signal.SIGINT, partial(sig_handler, server, web_app.mqtt_client))

    # Cria e starta o ioloop
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.start()
