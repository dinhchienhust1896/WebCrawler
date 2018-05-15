__author__ = 'chiendd'

import pika
import logging
from urlparse import urlparse
from json import dumps, loads
from time import gmtime, strftime
from ast import literal_eval
from bs4 import BeautifulSoup
import feedparser
from ConfigParser import RawConfigParser

list_host = []
list_tag = []
def callback(ch, method, properties, body):
    # Get data
    # {'url': url, 'code': code, 'content': content, 'type': 'Type'}
    json_data = loads(body)
    url = json_data['url']
    code = json_data['code']
    content = json_data['content']
    type = json_data['type']
    # Check type
    if type == 'Error':
        # Message Error
        err_data = {'error':{'url':url, 'code':code, 'content':content}}
        err_dump = dumps(err_data)
        channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=err_dump)
        logger.info('Done : (Error)-%s', url)
        print 'Done :(Error)-', url
    elif type == 'HTML':
        # Content HTML
        # Get domain
        tag_list_dump = ''
        parse = urlparse(url)
        domain = parse.hostname
        global list_host
        global list_tag
        for item in list_host:
            if item == domain:
                index = list_host.index(item)
                tag_list_dump = list_tag[index]
                break
        else:
            # Tag_list empty
            timecrawl = strftime("%Y-%m-%dT %H:%M:%S", gmtime())
            html_data = {'html':{'url':url, 'raw':content, 'content':'', 'code':code, 'time':timecrawl}}
            html_dump = dumps(html_data)
            channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=html_dump)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info('Done :(HTML)-%s', url)
            print 'Done :(HTML)-', url
            return
        # Tag_list exist
        soup = BeautifulSoup(content, 'html.parser')
        s = ','
        lst_content = []
        tag_list = literal_eval(tag_list_dump)
        for tag in tag_list:
            tag_data = soup.find(id=tag)
            if tag_data != None:
                tag_content = tag_data.get_text()
                lst_content.append(tag_content)
        html_content = s.join(lst_content)
        timecrawl = strftime("%Y-%m-%dT %H:%M:%S", gmtime())
        html_data = {'html':{'url':url, 'raw':content, 'content':html_content, 'code':code, 'time':timecrawl}}
        html_dump = dumps(html_data)
        channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=html_dump)
        logger.info('Done :(HTML)-%s', url)
        print 'Done :(HTML)-', url
    else:
        # Content RSS
        f = feedparser.parse(content)
        timecrawl = strftime("%Y-%m-%dT %H:%M:%S", gmtime())
        rss_data = {'rss':{'url':url, 'content':content, 'code':code, 'time':timecrawl}}
        rss_dump = dumps(rss_data)
        channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=rss_dump)
        # Get infomation of list item
        for entry in f.entries:
            title=''
            url_item = ''
            timepublish = ''
            catelogy = ''
            description = ''
            # Get title
            try:
                title = entry.title
            except AttributeError:
                pass
            # Get url
            try:
                url_item = entry.link
            except AttributeError:
                pass
            # Get time published
            try:
                time_strct = entry.published_parsed
                timepublish = strftime("%Y-%m-%dT %H:%M:%S", time_strct)
            except AttributeError:
                pass
            # Get category
            try:
                s = ','
                lst_category = []
                for tag in entry.tags:
                    lst_category.append(tag['term'])
                catelogy = s.join(lst_category)
            except AttributeError:
                pass
            # Get description
            try:
                description = entry.summary
            except AttributeError:
                pass
            # Send item to QUEUE_INSERT
            timecrawl = strftime("%Y-%m-%dT %H:%M:%S", gmtime())
            item_data = {'item': {'url_parent': url, 'title': title, 'url': url_item,
                                      'time_publish': timepublish, 'time': timecrawl,
                                      'catelogy': catelogy, 'description': description}}
            # Send content item to queue B
            item_dump = dumps(item_data)
            channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=item_dump)
        logger.info('Done : (RSS)-%s', url)
        print 'Done :(RSS)-',url
    ch.basic_ack(delivery_tag=method.delivery_tag)

if __name__ == '__main__':
    # Setup Log
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # Log - Create a file handler
    handler = logging.FileHandler('./log/WorkerParse.log')
    handler.setLevel(logging.INFO)
    # Log - Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # Log - Add the handlers to the logger
    logger.addHandler(handler)

    ##########################################
    # Start worker
    logger.info('================Start working================')
    # Get config file
    list_host = []
    list_tag = []
    config = RawConfigParser()
    config.read('./config/Config.ini')
    num_host = config.getint('Initialization', 'number_of_host')
    for i in range(num_host):
        hostname = 'Host' + (str)(i + 1)
        host = config.get(hostname, 'domain')
        list_host.append(host)
        taglist = config.get(hostname, 'tag_list')
        list_tag.append(taglist)
    # Create connection, channel to RabbitMQ
    rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    logger.info('Connect to RabbitMQ-server')
    channel_parse = rabbit.channel()
    channel_insert = rabbit.channel()
    logger.info('Create 2 channel : channel_parse, channel_insert')
    # Create message queue
    channel_insert.queue_declare(queue='QUEUE_INSERT', durable=True)
    logger.info('channel_insert create queue : QUEUE_INSERT (durable = True)')
    channel_parse.queue_declare(queue='QUEUE_PARSE', durable=True)
    logger.info('channel_parse create queue : QUEUE_PARSER (durable = True)')
    # Receive message from QUEUE_PARSE
    channel_parse.basic_consume(callback, queue='QUEUE_PARSE')
    logger.info('Start consuming. Queue : QUEUE_PARSE')
    print 'Start Consume!. QUEUE_PARSER'
    channel_parse.start_consuming()
