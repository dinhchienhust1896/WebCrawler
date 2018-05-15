__author__ = 'chiendd'

import pika
import logging
import os
from json import dumps, loads
from multiprocessing import Pool, current_process
from requests import get, head, HTTPError, ConnectionError, URLRequired
from ast import literal_eval
from ConfigParser import RawConfigParser

def consume():
	def callback(ch, method, properties, body):
		# {'url': URL, 'level': Level}
		json_data = loads(body)
		url = json_data['url']
		level = json_data['level']
		try:
			# HEAD
			header = head(url)

			for item in content_accept:
				try:
					if item in header.headers['Content-Type']:
						break
				except KeyError:
					break
			else:
				logger.error('PID : %d\tContent-type not found : %s', os.getpid(), url)
				ch.basic_ack(delivery_tag=method.delivery_tag)
				return
			# GET
			data_raw = get(url)
			code = data_raw.status_code
			data_dict = {'url': url, 'code': code, 'content': data_raw.text, 'level': level}
			print 'PID:',os.getpid(),'-', url
			logger.info('PID : %d\tSuccess : %s', os.getpid(), url)
		except HTTPError:
			logger.error('PID : %d\tHTTPError : %s', os.getpid(), url)
			data_dict = {'url': url, 'code': -1, 'content': 'HTTP Error!', 'level': level}
		except URLRequired:
			logger.error('PID : %d\tURLRequired : %s', os.getpid(), url)
			data_dict = {'url': url, 'code': -2, 'content': 'URL Error!', 'level': level}
		except ValueError:
			logger.error('PID : %d\tValueError : %s', os.getpid(), url)
			data_dict = {'url': url, 'code': -3, 'content': 'Value Error!', 'level': level}
		except ConnectionError:
			logger.error('PID : %d\tConnectionError : %s', os.getpid(), url)
			data_dict = {'url': url, 'code': -4, 'content': 'Connection Error!', 'level': level}
		json_data = dumps(data_dict)
		channel_raw.basic_publish(exchange='', routing_key='QUEUE_RAW', body=json_data)
		# ACK
		ch.basic_ack(delivery_tag=method.delivery_tag)
	print 'Create process. PID:', os.getpid()
	# Get Config.ini
	logger.info('Create process. Process ID :%d', os.getpid())
	configparser = RawConfigParser()
	configparser.read('./config/Config.ini')
	# Get content-accept from Config.ini
	data = configparser.get('WorkerCollect', 'content_accept')
	content_accept = literal_eval(data)
	# Create connection, channel to rabbitmq-server
	rabbit = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel_request = rabbit.channel()
	channel_raw = rabbit.channel()
	# Create Queue
	channel_raw.queue_declare(queue='QUEUE_RAW', durable=True)
	channel_request.queue_declare(queue='QUEUE_REQUEST', durable=True)
	channel_request.basic_qos(prefetch_count=1)
	channel_request.basic_consume(callback, queue='QUEUE_REQUEST')
	# Start consume
	channel_request.start_consuming()

if __name__ == '__main__':
	# Setup Log
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.INFO)
	# Log - Create a file handler
	handler = logging.FileHandler('./log/WorkerCollect.log')
	handler.setLevel(logging.INFO)
	# Log - Create a logging format
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	# Log - Add the handlers to the logger
	logger.addHandler(handler)

	##########################################
	# Start worker
	logger.info('================Start working================')
	# Get Config.ini
	config = RawConfigParser()
	config.read('./config/Config.ini')
	# Get number_of_worker_thread from Config.ini to create multi worker
	# Section WorkerCollect (number_of_collector_thread)
	workers = config.getint('WorkerCollect', 'number_of_collector_thread')
	print 'Create',workers,'process!\nStart consuming QUEUE_REQUEST!'
	logger.info('Start consuming. Queue : QUEUE_REQUEST')
	# Create multi process
	pool = Pool(processes=workers)
	for i in xrange(0, workers):
		pool.apply_async(consume)
	# Stay alive, wait KeyboardInterrupt to end program
	logger.info('Create %d process', workers)
	try:
		while True:
			continue
	except KeyboardInterrupt:
		# End
		print 'Exitting!'
		logger.info('Terminate %d process', workers)
		pool.terminate()
		pool.join()
