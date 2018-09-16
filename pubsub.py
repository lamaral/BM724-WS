"""
The pubsub module provides interface for the mosquitto client.

It provides classes to create mqtt clients vai paho-mqtt library to connect to mosquitto broker server,
interact with and publish/subscribe to mosquitto via creating topics, methods to publish, subscribe/consume,
stop consuming, start publishing, start connection, stop connection,  acknowledge delivery by publisher,
acknowledge receiving of messages by consumers and also add callbacks for various other events.

"""


import json
import logging
import base64
import os
import msgpack

import tornado.ioloop
import paho.mqtt.client as mqtt

from collections import deque


LOGGER = logging.getLogger("MosquittoClient")

# global tornado main ioloop object
ioloop = tornado.ioloop.IOLoop.instance()

# defining IO events
WRITE = tornado.ioloop.IOLoop.WRITE
READ = tornado.ioloop.IOLoop.READ
ERROR = tornado.ioloop.IOLoop.ERROR

WSclients = set()

buffer = deque(maxlen=200)


class MosquittoClient(object):
    """
    This is a Mosquitto Client class that will create an interface to connect to mosquitto
    by creating mqtt clients.

    It provides methods for connecting, diconnecting, publishing, subscribing, unsubscribing and
    also callbacks related to many different events like on_connect, on_message, on_publish, on_subscribe,
    on_unsubcribe, on_disconnect.

    """

    def __init__(self, clientid=None, clean_session=True, host='localhost', port=1883, keepalive=60):

        self._clientid = clientid or self._genid()
        self._clean_session = clean_session
        self._host = host
        self._port = port
        self._keepalive = keepalive

        self._connected = False
        self._connecting = False
        self._closing = False
        self._closed = False
        self._connection = None
        self._client = None
        self.websocket = None
        self._subNo = 0
        self._sock = None
        self._ioloopClosed = False
        self._schedular = None

    def _genid(self):
        """
        Method that generates unique clientids by calling base64.urlsafe_b64encode(os.urandom(32)).replace('=', 'e').

        :return:        Returns a unique urlsafe id
        :rtype:         string

        """

        return base64.urlsafe_b64encode(os.urandom(32)).decode().replace('=', 'e')

    def start(self):
        """
        Method to start the mosquitto client by initiating a connection  to mosquitto broker
        by using the connect method and staring the network loop.

        """

        LOGGER.info('starting the mosquitto connection')

        self.setup_connection()
        self.setup_callbacks()

        # self._connection is the return code of the connection, success, failure, error. Success = 0
        self._connection = self.connect()

        # print 'self._connection : ', self._connection

        if self._connection == 0:
            # Start paho-mqtt mosquitto Event/IO Loop
            LOGGER.info('Startig IOLoop for client : %s ' % self)
            self.start_ioloop()

            # Start schedular for keeping the mqtt connection opening by chekcing keepalive, and request/response with PINGREQ/PINGRESP
            self.start_schedular()

        else:
            self._connecting = False

            LOGGER.warning('Connection for client :  %s  with broker Not Established ' % self)

    def setup_connection(self):
        """
        Method to setup the extra options like username,password, will set, tls_set etc
        before starting the connection.

        """

        self._client = self.create_client()

    def create_client(self):
        """
        Method to create the paho-mqtt Client object which will be used to connect
        to mosquitto.

        :return:        Returns a mosquitto mqtt client object
        :rtype:         paho.mqtt.client.Client

        """

        return mqtt.Client(client_id=self._clientid, clean_session=self._clean_session)

    def setup_callbacks(self):
        """
        Method to setup all callbacks related to the connection, like on_connect,
        on_disconnect, on_publish, on_subscribe, on_unsubcribe etc.

        """

        self._client.on_connect = self.on_connect
        self._client.on_disconnect = self.on_disconnect
        self._client.on_message = self.on_message

    def connect(self):
        """
        This method connects to Mosquitto via returning the
        connection return code.

        When the connection is established, the on_connect callback
        will be invoked by paho-mqtt.

        :return:        Returns a mosquitto mqtt connection return code, success, failure, error, etc
        :rtype:         int

        """

        if self._connecting:
            LOGGER.warning('Already connecting to MQTT Broker')
            return

        self._connecting = True

        if self._connected:
            LOGGER.warning('Already connected to MQTT Broker')
        else:
            LOGGER.info('Connecting to MQTT Broker on %s:%d' % (self._host, self._port))
            return self._client.connect(host=self._host, port=self._port, keepalive=self._keepalive)

    def on_connect(self, client, userdata, flags, rc):
        """
        This is a Callback method and is called when the broker responds to our
        connection request.

        :param      client:     the client instance for this callback
        :param      userdata:   the private user data as set in Client() or userdata_set()
        :param      flags:      response flags sent by the broker
        :type       flags:      dict
        :param      rc:         the connection result
        :type       rc:         int

        flags is a dict that contains response flags from the broker:

        flags['session present'] - this flag is useful for clients that are using clean session
        set to 0 only. If a client with clean session=0, that reconnects to a broker that it has
        previously connected to, this flag indicates whether the broker still has the session
        information for the client. If 1, the session still exists.

        The value of rc indicates success or not:

        0: Connection successful 1: Connection refused - incorrect protocol version
        2: Connection refused - invalid client identifier 3: Connection refused - server unavailable
        4: Connection refused - bad username or password 5: Connection refused - not authorised
        6-255: Currently unused.

        """

        if self._connection == 0:
            self._connected = True
            LOGGER.info('Connection for client :  %s  with broker established, Return Code : %s ' % (client, str(rc)))

            # start subscribing to topics
            self.subscribe()

        else:
            self._connecting = False
            LOGGER.warning('Connection for client :  %s  with broker Not Established, Return Code : %s ' % (client, str(rc)))

    def start_ioloop(self):
        """
        Method to start ioloop for paho-mqtt mosquitto clients so that it can
        process read/write events for the sockets.

        Using tornado's ioloop, since if we use any of the loop*() function provided by
        phao-mqtt library, it will either block the entire tornado thread, or it will
        keep on creating separate thread for each client if we use loop_start() fucntion.

        We don't want to block thread or to create so many threads unnecessarily given
        python GIL.

        Since the separate threads calls the loop() function indefinitely, and since its doing
        network io, its possible it may release GIL, but I haven't checked that yet, if that
        is the case, we can very well use loop_start().Pattern

        But for now we will add handlers to tornado's ioloop().

        """

        # the socket conection of the present mqtt mosquitto client object
        self._sock = self._client.socket()

        # adding tornado iooloop handler
        events = READ | WRITE | ERROR

        # print 'adding tornado handler now'

        if self._sock:
            # print 'self._sock is present, hence adding handler'
            if self._sock.fileno() not in ioloop.handlers:
                ioloop.add_handler(self._sock.fileno(), self._events_handler, events)
            else:
                ioloop.update_handler(self._sock.fileno(), events)
        else:
            LOGGER.warning('client socket is closed already')

    def stop_ioloop(self):
        """
        Method to stop ioloop for paho-mqtt mosquitto clients so that it cannot
        process any more read/write events for the sockets.

        Actually the paho-mqtt mosquitto socket has been closed, so bascially this
        method removed the tornaod ioloop handler for this socket.

        """

        self._sock = self._client.socket()

        # # removing tornado iooloop handler
        # print 'removing tornado handler now'

        if self._sock:
            # print 'self._sock is present, hence removing handler'
            ioloop.remove_handler(self._sock.fileno())
            # updating close state of ioloop
            self._ioloopClosed = True
        else:
            LOGGER.warning('client socket is closed already')

    def _events_handler(self, fd, events):
        """
        Handle IO/Event loop events, processing them.

        :param      fd:             The file descriptor for the events
        :type       fd:             int
        :param      events:         Events from the IO/Event loop
        :type       events:         int

        """

        self._sock = self._client.socket()
        # print 'self._sock : ', self._sock

        if not self._sock:
            LOGGER.error('Received events on closed socket: %r', fd)
            return

        if events & WRITE:
            # LOGGER.info('Received WRITE event')

            # handler write events by calling loop_read() method of paho-mqtt client
            self._client.loop_write()

        if events & READ:
            # LOGGER.info('Received READ event')

            # handler write events by calling loop_read() method of paho-mqtt client
            self._client.loop_read()

        if events & ERROR:
            LOGGER.error('Error event for socket : %s and client : %s ' % (self._sock, self._client))

    def start_schedular(self):
        """
        This method calls Tornado's PeriodicCallback to schedule a callback every few seconds,
        which calls paho mqtt client's loop_misc() function which keeps the connection open by
        checking for keepalive value and by keep sending pingreq and pingresp to moqsuitto broker.

        """

        LOGGER.info('Starting Scheduler for client : %s ' % self)

        # torndao ioloop shcedular
        self._schedular = tornado.ioloop.PeriodicCallback(callback=self._client.loop_misc, callback_time=10000)

        # start the schedular
        self._schedular.start()

    def stop_schedular(self):
        """
        This method calls stops the tornado's periodicCallback Schedular loop.

        """

        LOGGER.info('Stoping Scheduler for client : %s ' % self)

        # stop the schedular
        self._schedular.stop()

    def disconnect(self):
        """
        Method to disconnect the mqqt connection with mosquitto broker.

        on_disconnect callback is called as a result of this method call.

        """

        if self._closing:
            LOGGER.warning('Connection for client :  %s  already disconnecting..' % self)

        else:
            self._closing = True

            if self._closed:
                LOGGER.warning('Connection for client :  %s  already disconnected ' % self)

            else:
                self._client.disconnect()

    def on_disconnect(self, client, userdata, rc):
        """
        This is a Callback method and is called when the client disconnects from
        the broker.

        """

        LOGGER.info('Connection for client :  %s  with broker cleanly disconnected with return code : %s ' % (client, str(rc)))

        self._connecting = False
        self._connected = False
        self._closing = True
        self._closed = True

        # stopping ioloop - actually mqtt ioloop stopped, not the real torando ioloop,
        # just removing handler from tornado ioloop
        self.stop_ioloop()

        # stoppig shechular
        self.stop_schedular()

        if self._ioloopClosed:
            self._sock = None

    def subscribe(self):
        """
        This method sets up the mqtt client to start subscribing to topics by accepting a list of tuples
        of topic and qos pairs.

        The on_subscribe method is called as a callback if subscribing is succesfull or if it unsuccessfull, the broker
        returng the suback frame.

        :param      :topic_list:    a tuple of (topic, qos), or, a list of tuple of format (topic, qos).
        :type       :topic_list:    list or tuple


        """

        LOGGER.info('Client : %s started Subscribing ' % self)
        topic_list = [
            # ("Master/7242/Session/Session-Stop", 2),
            # ("Master/7242/Session/Session-Start", 2),
            # ("Master/7242/Session/Session-Update", 2),
            # ("Master/7242/Session/Loss-Rate", 2)
            ("BRHeard/#", 1)
        ]
        LOGGER.info('Subscribing to topic_list : %s ' % str(topic_list))
        self._client.subscribe(topic_list)

    def unsubscribe(self, topic_list=None):
        """
        This method sets up the mqtt client to unsubscribe to topics by accepting topics as string or list.

        The on_unsubscribe method is called as a callback if unsubscribing is succesfull or if it unsuccessfull.

        :param      topic_list:        The topics to be unsubscribed from
        :type       topic_list:         list of strings(topics)

        """

        LOGGER.info('clinet : %s started Unsubscribing ' % self)
        self._client.unsubscribe(topic_list)

    def publish(self, topic, msg=None, qos=2, retain=False):
        """
        If the class is not stopping, publish a message to MosquittoClient.

        on_publish callback is called after broker confirms the published message.

        :param  topic:  The topic the message is to published to
        :type   topic:  string
        :param  msg:    Message to be published to broker
        :type   msg:    string
        :param  qos:    the qos of publishing message
        :type   qos:    int (0, 1 or 2)
        :param  retain: Should the message be retained or not
        :type   retain: bool

        """

        # LOGGER.info('Publishing message')

        # converting message to json, to pass the message(dict) in acceptable format (string)
        if isinstance(msg, bytes) or isinstance(msg, str):
            payload = msg
        else:
            payload = json.dumps(msg, ensure_ascii=False)

        self._client.publish(topic=topic, payload=payload, qos=qos, retain=retain)

    def on_message(self, client, userdata, msg):
        """
        This is a Callback method and is called  when a message has been received on a topic
        [public/msgs] that the client subscribes to.

        :param      client:         the client who initiated the publish method
        :param      userdata:       the userdata associated with the client during its creation
        :param      msg:            the message sent by the broker
        :type       mid:            string or json encoded string

        """

        try:
            LOGGER.debug('Received message with mid : %s from topic : %s with qos :  %s and retain = %s ' % (str(msg.mid), msg.topic, str(msg.qos), str(msgpack.unpackb(msg.payload))))
            payload = msgpack.unpackb(msg.payload, encoding='utf-8')
        except UnicodeDecodeError:
            LOGGER.warning('Error decoding message: %s' % msg.payload)
            return


        message = {}

        message['Event'] = msg.topic.split("/")[1]
        message['Network'] = payload[0]
        message['Session'] = payload[1]
        message['CreationTime'] = payload[2]
        message['UpdateTime'] = payload[3]
        message['LinkName'] = payload[4]
        message['LinkKind'] = payload[5]
        message['LinkID'] = payload[6]
        message['LinkSlot'] = payload[7]
        message['CallType'] = payload[8]
        message['SourceID'] = payload[9]
        message['DestinationID'] = payload[10]
        message['Priority'] = payload[11]
        message['Route'] = payload[12]
        message['State'] = payload[13]
        message['DataCount'] = payload[14]
        message['SignalStrength'] = payload[15]
        message['ErrorRatio'] = payload[16]
        message['LossCount'] = payload[17]
        message['TotalCount'] = payload[18]
        message['ReflectorID'] = payload[19]
        message['RepeaterCall'] = payload[20]
        message['SourceCall'] = payload[21]
        message['SourceName'] = payload[22]
        message['DestinationCall'] = payload[23]
        message['DestinationName'] = payload[24]
        message['TalkerAlias'] = payload[25]

        jsonmessage = json.dumps(message)

        LOGGER.debug(jsonmessage)

        if message['Event'] == 'Session-Stop' or message['Event'] == 'Session-Start':
            buffer.append(jsonmessage)

        for socket in WSclients:
            socket.write_message(jsonmessage)

    def add_client(self, client):
        if client not in WSclients:
            WSclients.add(client)

    def remove_client(self, client):
        WSclients.remove(client)

    def get_buffer(self):
        return buffer

    def stop(self):
        """
        Cleanly shutdown the connection to Mosquitto by disconnecting the mqtt client.

        When mosquitto confirms disconection, on_disconnect callback will be called.

        """

        LOGGER.info('Stopping MosquittoClient object... : %s ' % self)
        self.disconnect()