__author__ = 'chiendd'

import pika
import logging
from json import dumps, loads
from py2neo import Relationship, Node, Graph
from urlparse import urlparse

def callback(ch, method, properties, body):
    data = loads(body)
    if 'relationship' in data.keys:
		# {'relationship': {'url': child_url, 'url_parent': url}}
		# Get data
        rela = data['relationship']
		# Create Parent, Child node
		parent_node = Node("URL", name = rela['url_parent'])
		child_node = Node("URL", name = rela['url'])
		relation = Relationship(parent_node, "Link", child_node)
		tx = graph.begin()
		tx.merge(parent_node, "URL", "name")
		tx.merge(child_node, "URL", "name")
		tx.merge(relation)
		tx.commit()
		logger.info('Create URL node : %s', rela['url_parent'])
		logger.info('Create URL node : %s', rela['url'])
	elif 'error' in data.keys():
		# {'error':{'url':url, 'code':code, 'content':content}}
		# Get data
		err_data = data['error']
		# Modify Error URL node
		err_node = Node("URL", name = err_data['url'])
		err_node['type'] = 'URL Error'
		err_node['code'] = err_data['code']
		err_node['description'] = err_data['content']
		tx = graph.begin()
		tx.merge(err_node, "URL", "name")
		logger.info('Modify URL node : %s', err_data['url'])
		# Create Domain node
		parse = urlparse(err_data['url'])
		domain = Node("DOMAIN", name = parse.hostname)
		# Create Relationship to DOMAIN
		relation = Relationship(domain, "Have", err_node)
		tx.merge(domain, "DOMAIN", "name")
		tx.merge(relation)
		tx.commit()
		logger.info('Create DOMAIN node : %s', parse.hostname)
		print 'Create URL node :',err_data['url']
	elif 'html' in data.keys():
		# {'html':{'url':url, 'raw':content, 'content':'content', 'code':code, 'time':timecrawl}}
		# Get data
		# Modify HTML node
		web_data = data['html']
		web_node = Node("URL", name = web_data['url'])
		web_node['type'] = 'HTML'
		web_node['raw'] = web_data['raw']
		web_node['content'] = web_data['content']
		web_node['code'] = web_data['code']
		web_node['time'] = web_data['time']
		tx = graph.begin()
		tx.merge(web_node, "URL", "name")
		logger.info('Modify URL node : %s', web_data['url'])
		# Create Node Domain
		parse = urlparse(web_data['url'])
		domain = Node("DOMAIN", name = parse.hostname)
		# Create Relationship to DOMAIN
		relation = Relationship(domain, "Have", web_node)
		tx.merge(domain, "DOMAIN", "name")
		tx.merge(relation)
		tx.commit()
		logger.info('Create DOMAIN node : %s', parse.hostname)
		print 'Create URL node :',web_data['url']
	elif 'rss' in data.keys():
		# {'rss':{'url':url, 'content':content, 'code':code, 'time':timecrawl}}
		# Get data
		rss_data = data['rss']
		# Modify RSS node
		rss_node = Node("URL", name = rss_data['url'])
		rss_node['type'] = 'RSS'
		rss_node['raw'] = rss_data['content']
		rss_node['code'] = rss_data['code']
		rss_node['time'] = rss_data['time']
		tx = graph.begin()
		tx.merge(rss_node, "URL", "name")
		logger.info('Modify URL node : %s', rss_data['url'])
		# Create Node Domain
		parse = urlparse(rss_data['url'])
		domain = Node("DOMAIN", name = parse.hostname)
		# Create Relationship to DOMAIN
		relation = Relationship(domain, "Have", rss_node)
		tx.merge(domain, "DOMAIN", "name")
		tx.merge(relation)
		tx.commit()
		logger.info('Create DOMAIN node : %s', parse.hostname)
		print 'Create URL node :',rss_data['url']
	else :
		# {'item': {'url_parent': url, 'title': title, 'url': url_item,
        #                              'time_publish': timepublish, 'time': timecrawl,
        #                              'category': category, 'description': description}}
		# Get data
		item_data = data['item']
		# Create ITEM node
		item_node = Node("ITEM", name = item_data['title'])
		item_node['publish'] = item_data['time_publish']
		item_node['category'] = item_data['category']
		item_node['description'] = item_data['description']
		item_node['crawl'] = item_data['time']
		item_node['link'] = item_data['url']
		tx = graph.begin()
		tx.merge(item_node, "ITEM", "name")
		logger.info('Create Item node : %s (Parent : %s)', item_data['url'], item_data['url_parent'])
		# Create Parent Node
		parent_node = Node("URL", name = item_data['url_parent'])
		tx.merge(parent_node, "URL", "name")
		# Create Relationship to Parent Node
		item_rela = Relationship(parent_node, "Have-item", item_node)
		tx.merge(item_rela)
		tx.commit()
		if item_data['title'] is None:
			print 'Create ITEM node :',item_data['title']
		else:
			print 'Create ITEM node :',item_data['url']
			
	# ACK
	ch.basic_ack(delivery_tag=method.delivery_tag)
	
if __name__ == '__main__':
    # Setup Log
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # Log - Create a file handler
    handler = logging.FileHandler('./log/WorkerInsert.log')
    handler.setLevel(logging.INFO)
    # Log - Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # Log - Add the handlers to the logger
    logger.addHandler(handler)

    ##########################################
    # Start worker
    logger.info('================Start working================')
    # Connect to Neo4j
    graph = Graph(host='localhost', password='12345678')
    # Create connection, channel to RabbitMQ
    rabbit = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    logger.info('Connect to RabbitMQ-server')
    channel_insert = rabbit.channel()
    logger.info('Create channel : channel_insert')
    # Create message queue
    channel_insert.queue_declare(queue='QUEUE_INSERT', durable=True)
    logger.info('channel_insert create queue : QUEUE_INSERT (durable = True)')
    # Receive message from QUEUE_INSERT
    channel_insert.basic_consume(callback, queue='QUEUE_INSERT')
    logger.info('Start consuming. Queue : QUEUE_INSERT')
    print 'Start Consume!. QUEUE_INSERT'
    channel_insert.start_consuming()