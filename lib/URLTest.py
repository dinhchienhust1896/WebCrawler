__author__ = 'chiendd'

from redis import Redis
from AreaCrawl import AreaCrawl
from URLBlock import URLBlock
from Robot import Robot
from urlparse import urlparse
from ast import literal_eval
from re import match


class URLTest:
	def __init__(self, url, level, parent):
		self.url = url
		self.level = level
		self.parent = parent
		self.r = Redis('localhost', 6379)
		self.new_level = 0
		self.log = ''
	# Test 1 : Already crawl
	def test_already_crawl(self):
		iscrawl = self.r.get(self.url)
		if iscrawl is None:
			self.r.set(self.url, 'Test')
			return True
		else:
			return False

	# Test 2 : URL Block
	def test_url_block(self):
		try:
			block = URLBlock(self.url)
			if block.test():
				return True
			else:
				return False
		except TypeError:
			return False

	# Test 3 : Area crawl
	def test_area_crawl(self):
		area = AreaCrawl(self.parent, self.url, self.level)
		if area.is_accept():
			self.new_level = area.new_level()
			return True
		else:
			return False

	# Test 4 : Robots.txt
	def test_robots(self):
		parser = urlparse(self.url)
		domain = parser.hostname
		isexistrobot = self.r.get(domain)
		if isexistrobot is None:
			# Collect Robots.txt
			robot = Robot(self.url)
			lst_rule = robot.get_rule()
			# Save to Redis
			self.r.set(domain, lst_rule)
			if robot.is_allowed(self.url):
				return True
			else:
				return False
		else:
			lst_rule = literal_eval(isexistrobot)
			path = parser.path
			for item in lst_rule:
				if '.' in item:
					item = item.replace('.', '\.')
				if '*' in item:
					item = item.replace('*', '.*')
				if '?' in item:
					item = item.replace('?', '\?')
				ro = match(item, path)
				if ro is not None:
					return False
			else:
				return True

	# Total Test
	def test(self):
		if not self.test_already_crawl():
			self.log = 'Already crawl'
			return False
		elif not self.test_url_block():
			self.log = 'URL Block'
			return False
		elif not self.test_area_crawl():
			self.log = 'Area level'
			return False
		elif not self.test_robots():
			self.log = 'Robot'
			return False
		else:
			return True
