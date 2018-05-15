from requests import *
from urlparse import urlparse
from re import match
from requests.exceptions import ProxyError, InvalidURL, InvalidSchema, SSLError

existRobot = True
class Robot:
	def __init__(self,url):
		parsed_uri = urlparse(url)
		self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
		self.url_robot = self.domain+'robots.txt'
		self.netloc = parsed_uri.netloc
		self.rule = []
		global existRobot
		try:
			r = get(self.url_robot)
			if r.encoding == 'utf-8':
				self.text = r.text.encode('utf-8')
			else :
				self.text = r.text
			# Get Disallow URL
			# Find firt
			i = self.text.find('User-agent: *')
			self.robot = self.text[i+14:]
	 		# Find end
			i = self.robot.find('User-agent:')
			self.robot = self.robot[:i]
			lines = self.robot.split('\n')
			for line in lines:
				if not line:
					continue
				if line[0] == '#':
					continue
				rule = line.split(' ')
				if rule[0] == 'Disallow:':
					try:
						self.rule.append(rule[1])
					except IndexError:
						pass
		except ProxyError:
			existRobot = False
		except InvalidURL:
			existRobot = False
		except InvalidSchema:
			existRobot = False
		except SSLError:
			existRobot = False
	def get_domain(self):
		return self.domain

	def get_home(self):
		return self.netloc

	def get_rule(self):
		return self.rule

	def is_allowed(self, testurl):
		global existRobot
		if existRobot:
			parsed_uri = urlparse(testurl)
			path = parsed_uri.path
			# Test
			for item in self.rule:
				if '.' in item:
					item = item.replace('.', '\.')
				if '*' in item:
					item = item.replace('*','.*')
				if '?' in item:
					item = item.replace('?','\?')
				r = match(item, path)
				if r != None:
					return False
			else:
				return True
		else:
			return True
        
