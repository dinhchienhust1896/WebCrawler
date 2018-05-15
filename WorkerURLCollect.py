__author__ = 'chiendd'

import pika
import logging
from json import dumps, loads
from bs4 import BeautifulSoup
from urlparse import urlparse
from redis import Redis
import feedparser
from sys import path
from os import getcwd

# Import URLTest
libdir = getcwd() + '/lib'
path.insert(0, libdir)
from URLTest import URLTest


def callback(ch, method, properties, body):
	# Get data
	json_data = loads(body)
	url = json_data['url']
	code = json_data['code']
	content = json_data['content']
	level = json_data['level']
	# Check Error
	if code < 200 or code > 299:
		logger.info('Deny-(ConnErr:%d) : %s', code, url)
		data_dict = {'url': url, 'code': code, 'content': content, 'type': 'Error'}
		dump_data = dumps(data_dict)
		channel_parse.basic_publish(exchange='', routing_key='QUEUE_PARSE', body=dump_data)
		ch.basic_ack(delivery_tag=method.delivery_tag)
	else:
		# Check RSS or HTML
		soup = BeautifulSoup(content, 'html.parser')
		ishtml = soup.find('html')
		if ishtml is not None:
			# HTML
			# Find all url in HTML
			parsed = urlparse(url)
			hostname = parsed.hostname
			num_link = 0
			num_link_pass = 0
			logger.info('URL Collect (HTML): %s', url)
			for link in soup.find_all('a', href=True):
				num_link += 1
				if link['href'].find('http') == -1:
					child_url = url[:url.find(hostname)] + hostname + link['href']
					test = URLTest(child_url, level, url)
					if test.test():
						# Allow
						logger.info('\tPass : %s', child_url)
						new_level = test.new_level
						# Send to QUEUE_REQUEST
						data_dict = {'url': child_url, 'level': new_level}
						dump_data = dumps(data_dict)
						channel_request.basic_publish(exchange='', routing_key='QUEUE_REQUEST', body=dump_data,
                                                      properties=pika.BasicProperties(delivery_mode=2))
						# Send Relationship to QUEUE_INSERT
						json_data = {'relationship': {'url': child_url, 'url_parent': url}}
						dump_data = dumps(json_data)
						channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=dump_data)
						print '\tPass :(Level:', new_level, ')\t', child_url
						num_link_pass += 1
					else:
						logger.info('\tDeny-(%s) : %s', test.log, child_url)
				else:
					# Test URL
					test = URLTest(link['href'], level, url)
					if test.test():
						# Allow
						logger.info('\tPass : %s', link['href'])
						new_level = test.new_level
						# Send to QUEUE_REQUEST
						data_dict = {'url': link['href'], 'level': new_level}
						dump_data = dumps(data_dict)
						channel_request.basic_publish(exchange='', routing_key='QUEUE_REQUEST', body=dump_data,
                                                      properties=pika.BasicProperties(delivery_mode=2))
						# Send Relationship to QUEUE_INSERT
						json_data = {'relationship': {'url': link['href'], 'url_parent': url}}
						dump_data = dumps(json_data)
						channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=dump_data)
						print '\tPass :(Level:', new_level, ')\t', link['href']
						num_link_pass += 1
					else:
						logger.info('\tDeny-(%s) : %s', test.log, link['href'])
			# Done
			print 'URL : ', url, '\tFound :', str(num_link), 'link. Pass :', str(num_link_pass)
			logger.info('URL : %s - Found : %d, Pass : %d', url, num_link, num_link_pass)
			print '================================================'
			# Forward to QUEUE_PARSE
			data_dict = {'url': url, 'code': code, 'content': content, 'type': 'HTML'}
			dump_data = dumps(data_dict)
			channel_parse.basic_publish(exchange='', routing_key='QUEUE_PARSE', body=dump_data)
		else:
			# RSS
			logger.info('URL Collect (RSS): %s', url)
			f = feedparser.parse(content)
			# Find all url in entries
			num_link = 0
			num_link_pass = 0
			for entry in f.entries:
				try:
					url_item = entry.link
					num_link += 1
					# Test URL
					test = URLTest(url_item, level, url)
					if test.test():
						# Allow
						logger.info('\tPass : %s', url_item)
						new_level = test.new_level
						# Send to QUEUE_REQUEST
						data_dict = {'url': url_item, 'level': new_level}
						dump_data = dumps(data_dict)
						channel_request.basic_publish(exchange='', routing_key='QUEUE_REQUEST', body=dump_data,
                                                      properties=pika.BasicProperties(delivery_mode=2))
						# Send Relationship to QUEUE_INSERT
						json_data = {'relationship': {'url': url_item, 'url_parent': url}}
						dump_data = dumps(json_data)
						channel_insert.basic_publish(exchange='', routing_key='QUEUE_INSERT', body=dump_data)
						print '\tPass :(Level:', new_level, ')\t', url_item
						num_link_pass += 1
					else:
						logger.info('\tDeny-(%s) : %s', test.log, url_item)
				except AttributeError:
					pass
			print 'URL : ', url, '\tFound :', str(num_link), 'link. Pass :', str(num_link_pass)
			logger.info('URL : %s - Found : %d, Pass : %d', url, num_link, num_link_pass)
			print '================================================'
			# Forward to QUEUE_PARSE
			data_dict = {'url': url, 'code': code, 'content': content, 'type': 'RSS'}
			dump_data = dumps(data_dict)
			channel_parse.basic_publish(exchange='', routing_key='QUEUE_PARSE', body=dump_data)
		# Done Collect URL
		ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    # Setup Log
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # Log - Create a file handler
    handler = logging.FileHandler('./log/WorkerURLCollect.log')
    handler.setLevel(logging.INFO)
    # Log - Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # Log - Add the handlers to the logger
    logger.addHandler(handler)

    ##########################################
    # Start worker
    logger.info('================Start working================')
    # Collect to Redis
    r = Redis('localhost', 6379)
    logger.info('Connect to Redis')
    # Create connection, channel to RabbitMQ
    rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    logger.info('Connect to RabbitMQ-server')
    channel_request = rabbit.channel()
    channel_raw = rabbit.channel()
    channel_parse = rabbit.channel()
    channel_insert = rabbit.channel()
    logger.info('Create 4 channel : channel_request, channel_raw, channel_parse, channel_insert')
    # Create message queue
    channel_parse.queue_declare(queue='QUEUE_PARSE', durable=True)
    logger.info('channel_parse create queue : QUEUE_PARSER (durable = True)')
    channel_request.queue_declare(queue='QUEUE_REQUEST', durable=True)
    logger.info('channel_request create queue : QUEUE_REQUEST (durable = True)')
    channel_raw.queue_declare(queue='QUEUE_RAW', durable=True)
    logger.info('channel_raw create queue : QUEUE_RAW (durable = True)')
    channel_insert.queue_declare(queue='QUEUE_INSERT', durable=True)
    logger.info('channel_insert create queue : QUEUE_INSERT (durable = True)')
    # Receive message from QUEUE_RAW
    channel_raw.basic_consume(callback, queue='QUEUE_RAW')
    logger.info('Start consuming. Queue : QUEUE_RAW')
    print 'Start Consume!. QUEUE_RAW'
    channel_raw.start_consuming()
