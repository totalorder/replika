# encoding: utf-8
import logging


class HierarchyLogger(object):
    def __init__(self, get_prefix, parent=None):
        self.get_prefix = get_prefix
        self.parent = parent
        self.on = True

    def off(self):
        self.on = False

    def on(self):
        self.on = True

    def is_on(self):
        return self.on and (self.parent.is_on() if self.parent else True)

    def prefix(self):
        if self.parent is not None:
            return self.parent.prefix() + ", " + self.get_prefix()
        else:
            return self.get_prefix()

    def info(self, logstr, *args, **kwargs):
        if self.is_on():
            logging.info(self.prefix() + ": " + logstr, *args, **kwargs)

    def warn(self, logstr, *args, **kwargs):
        if self.is_on():
            logging.warn(self.prefix() + ": " + logstr, *args, **kwargs)

    def error(self, logstr, *args, **kwargs):
        if self.is_on():
            logging.error(self.prefix() + ": " + logstr, *args, **kwargs)

    def debug(self, logstr, *args, **kwargs):
        if self.is_on():
            logging.debug(self.prefix() + ": " + logstr, *args, **kwargs)


class Sentinel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "{} ({})".format(self.name, id(self))