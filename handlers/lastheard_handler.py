import tornado.websocket


class LastHeardHandler(tornado.websocket.WebSocketHandler):
    def check_origin(self, origin):
        return True

    def open(self):
        buffer = self.application.mqtt_client.get_buffer()
        for entry in buffer:
            self.write_message(entry)
        self.application.mqtt_client.add_client(self)

    def on_close(self):
        self.application.mqtt_client.remove_client(self)

    def on_message(self, message):
        print("got message " + message)
