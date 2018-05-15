__author__ = 'chiendd'

from ConfigParser import RawConfigParser
from ast import literal_eval
from urlparse import urlparse

class URLBlock:
	def __init__(self, url):
		self.url = url
		configparser = RawConfigParser()
		configparser.read('./config/Config.ini')
		data = configparser.get('WorkerCollect', 'host_block')
		self.block = []
		self.block = literal_eval(data)
	def test(self):
		parser = urlparse(self.url)
		domain = parser.hostname
		for item in self.block:
			if item in domain:
				return False
			else:
				continue
		else:
			return True
