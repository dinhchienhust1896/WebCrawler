__author__ = 'chiendd'

from ConfigParser import RawConfigParser
import pika
from json import dumps
from ast import literal_eval

if __name__ == '__main__':
    # Create connection, channel to RabbitmQ
    rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = rabbit.channel()
    # Create message queue
    channel.queue_declare(queue='QUEUE_REQUEST', durable=True)
    # Get Config.ini
    config = RawConfigParser()
    config.read('./config/Config.ini')
    num_host = config.getint('Initialization', 'number_of_host')
    for i in range(num_host):
        hostname = 'Host' + str(i + 1)
        url = config.get(hostname, 'url')
        lst_url = literal_eval(url)
        for item in lst_url:
            data_dict = {'url': item, 'level': 0}
            dump_data = dumps(data_dict)
            channel.basic_publish(exchange='', routing_key='QUEUE_REQUEST', body=dump_data)
            print 'Send :\t', dump_data
