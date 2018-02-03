import sys


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
