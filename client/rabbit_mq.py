# -*- coding: utf-8 -*-
import pika
import json
import time
from logging import getLogger

class BasicInteractor(object):
    """This is an example parent consumer/publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    EXCHANGE_TYPE = 'topic'

    def __init__(self, host, username, password, exchange, queue, routing_key_or_keys):
        """Create a new instance of the consumer class, passing in the AMQP
        parameters used to connect to RabbitMQ."""
        self.host = host
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queue = queue
        self.routing_keys = routing_key_or_keys if isinstance(routing_key_or_keys, (list,tuple)) else [routing_key_or_keys]
        self._connection = None
        self._channel = None
        self._closing = False

    def __init__1(self, amqp_url):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._url = amqp_url

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        getLogger(__name__).debug('Connecting to {} {} {}'.format(self.host, self.username, self.password))
        return pika.SelectConnection(pika.ConnectionParameters\
                                     (host=self.host,
                                      credentials=pika.PlainCredentials(self.username, self.password)),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)
        #return pika.SelectConnection(pika.URLParameters(self._url),
                                     #self.on_connection_open,
                                     #stop_ioloop_on_close=False)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        getLogger(__name__).debug('Closing connection')
        self._closing = True
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        getLogger(__name__).debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            getLogger(__name__).warning('Connection closed, reopening in 5 seconds: (%s) %s')
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        getLogger(__name__).debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        getLogger(__name__).debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        getLogger(__name__).warning('Channel was closed: %s', channel)
        if not self._closing:
            self.close_connection()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        getLogger(__name__).debug('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        getLogger(__name__).debug('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name, self.EXCHANGE_TYPE, durable=True)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        getLogger(__name__).debug('Exchange declared')
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        getLogger(__name__).debug('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        getLogger(__name__).debug('Binding %s to %s with %s',
                    self.exchange, self.queue, self.routing_keys)
        for routing_key in self.routing_keys:
            self._channel.queue_bind(self.on_bindok, self.queue,
                                     self.exchange, routing_key)

    def on_bindok(self, unused_frame):
        "Abstract method which must be overridden by descendant (publisher or consumer)"
        raise NotImplemented('Must be overridden by descendant.')

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        getLogger(__name__).debug('Closing the channel')
        if self._channel:
            self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        getLogger(__name__).debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        # First try to set up a connection. If it fails, retry until a connection is made:
        while not self._closing:
            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except Exception as e:
                getLogger(__name__).error(e)
                time.sleep(5)

class BasicPublisher(BasicInteractor):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """
    def __init__(self, *args, **kwargs):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        """
        super(BasicPublisher, self).__init__(*args, **kwargs)
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        getLogger(__name__).debug('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        getLogger(__name__).debug('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        getLogger(__name__).debug('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._stopping:
            return

        message = {u'مفتاح': u' قيمة',
                   u'键': u'值',
                   u'キー': u'値'}
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json',
                                          headers=message)

        self._channel.basic_publish(self.exchange, self.ROUTING_KEY,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        getLogger(__name__).debug('Published message # %i', self._message_number)
        self.schedule_next_message()

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        if self._stopping:
            return
        getLogger(__name__).debug('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        getLogger(__name__).debug('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def on_bindok(self, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        getLogger(__name__).debug('Queue bound')
        self.start_publishing()

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        getLogger(__name__).debug('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        getLogger(__name__).debug('Stopped')

class BasicConsumer(BasicInteractor):
    def __init__(self, host, username, password, exchange, queue, routing_key_or_keys, callback):
        super(BasicConsumer, self).__init__(host, username, password, exchange, queue, routing_key_or_keys)
        self._consumer_tag = None
        self.callback = callback

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        getLogger(__name__).debug('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        getLogger(__name__).debug('Queue bound')
        self.start_consuming()

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        getLogger(__name__).debug('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        getLogger(__name__).debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_message(self, channel, method, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        getLogger(__name__).debug('Received message # %s from %s: %s',
                    method.delivery_tag, properties.app_id, body)
        self.callback(channel, method, properties, body)
        self.acknowledge_message(method.delivery_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        getLogger(__name__).debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            getLogger(__name__).debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        getLogger(__name__).debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue)

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        getLogger(__name__).debug('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        getLogger(__name__).debug('Stopped')

def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
    #example = BasicPublisher('amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600')
    example = BasicConsumer('amqp://indroid:indroid@indroid.server:5672/%2F')
    try:
        example.run()
    except KeyboardInterrupt:
        example.stop()

if __name__ == '__main__':
    main()