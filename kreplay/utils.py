import sys
import time


def now():
    return int(round(time.time() * 1000))


class Log:
    @staticmethod
    def info(msg):
        print >> sys.stderr, msg

    @staticmethod
    def warn(msg):
        print >> sys.stderr, msg

    @staticmethod
    def error(msg):
        print >> sys.stdout, msg

    @staticmethod
    def debug(msg):
        # print >> sys.stdout, msg
        pass
