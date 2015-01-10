# encoding: utf-8
import logging


class HierarchyLogger(object):
    def __init__(self, get_prefix, parent=None):
        self.get_prefix = get_prefix
        self.parent = parent

    def prefix(self):
        if self.parent is not None:
            return self.parent.prefix() + u", " + self.get_prefix()
        else:
            return self.get_prefix()

    def info(self, logstr, *args, **kwargs):
        logging.info(self.prefix() + u": " + logstr, *args, **kwargs)

    def warn(self, logstr, *args, **kwargs):
        logging.warn(self.prefix() + u": " + logstr, *args, **kwargs)

    def error(self, logstr, *args, **kwargs):
        logging.error(self.prefix() + u": " + logstr, *args, **kwargs)

    def debug(self, logstr, *args, **kwargs):
        logging.debug(self.prefix() + u": " + logstr, *args, **kwargs)