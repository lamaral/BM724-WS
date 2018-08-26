import tornado.websocket


class LastHeardHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        print("Opened new connection")
        self.application.mqtt_client.add_client(self)

    def on_close(self):
        self.application.mqtt_client.remove_client(self)

    def on_message(self, message):
        print("got message " + message)
