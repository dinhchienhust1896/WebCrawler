__author__ = 'chiendd'

from urlparse import urlparse
from ConfigParser import RawConfigParser

class AreaCrawl:
    def __init__(self, parent, child, level):
        self.parent = parent
        self.child = child
        self.level = level
        parser = urlparse(parent)
        self.parent_domain = parser.hostname
        parser = urlparse(child)
        self.child_domain = parser.hostname
        self.list_host = []
        config = RawConfigParser()
        config.read('./config/Config.ini')
        self.area_inside = config.getint('Initialization', 'area_inside')
        self.area_outside = config.getint('Initialization', 'area_outside')
        num_host = config.getint('Initialization', 'number_of_host')
        for i in range(num_host):
            hostname = 'Host'+(str)(i+1)
            host = config.get(hostname, 'domain')
            self.list_host.append(host)
        self.parent_area = ''
        self.child_area = ''
        for i in self.list_host:
            if i == self.parent_domain:
                self.parent_area = 'Inside'
                break
        else:
            self.parent_area = 'Outside'
        for i in self.list_host:
            if i == self.child_domain:
                self.child_area = 'Inside'
                break
        else:
            self.child_area = 'Outside'

    def area_parent(self):
        return self.parent_area

    def area_child(self):
        return self.child_area

    def new_level(self):
        if self.parent_area =='Inside':
            if self.child_area =='Inside':
                return self.level +1
            else:
                return 0
        else:
            if self.child_area == 'Inside':
                return 0
            else:
                return self.level +1
    def is_accept(self):
        level = self.new_level()
        if self.child_area == 'Inside':
            if level > self.area_inside:
                return False
            else:
                return True
        else:
            if level > self.area_outside:
                return False
            else:
                return True
