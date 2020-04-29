import sys
# import argparse
import traceback as tb
from datetime import datetime
from enum import IntEnum
class Level(IntEnum):

	FINEST = 7
	FINER = 6
	FINE = 5
	DEBUG = 4
	INFO = 3
	WARN = 2
	ERROR = 1
	FATAL = 0

# Expose as global variables
FINEST = Level.FINEST
FINER = Level.FINER
FINE = Level.FINE
INFO = Level.INFO
WARN = Level.WARN
ERROR = Level.ERROR
FATAL = Level.FATAL
class Logger:
	# Add Levels as Logger attributes
	FINEST = Level.FINEST
	FINER = Level.FINER
	FINE = Level.FINE
	INFO = Level.INFO
	WARN = Level.WARN
	ERROR = Level.ERROR
	FATAL = Level.FATAL
	def __init__(self, module, level=Level.INFO):
		self.level = level
		self.module = module
	# Accepts str, int, or Level types.
	def set_listen_level(self, level):
		try:
			try:
				level = Level(int(level))
				self.level = level
			except ValueError:
				level = Level[str(level)]
				self.level = level
			except:
				self.error('Ran into an unexpected exception while trying to set level:\n %s' % (tb.format_exc(limit = 10)))
		except:
			self.error('Ran into an exception while trying to set level:\n %s' % (tb.format_exc(limit = 10)))
	# General log method.
	def log(self, message, level):
		if level <= self.level:
			print('[%s] [%s]: [%s] %s' % (datetime.now().strftime('%H:%M:%S'), level, self.module, message))
			# TODO: Add log.txt
	# Log methods for ease of use.
	def finest(self, message):
		self.log(message, Level.FINEST)
	def finer(self, message):
		self.log(message, Level.FINER)
	def fine(self, message):
		self.log(message, Level.FINE)
	def info(self, message):
		self.log(message, Level.INFO)
	def warn(self, message):
		self.log(message, Level.WARN)
	def error(self,message):
		self.log(message, Level.ERROR)
	def fatal(self, message):
		self.log(message, Level.FATAL)

	# Set's module for Logger object.
	def set_module(self, module):
		self.module = module

	def read_args(self):
		# parser = argparse.ArgumentParser()
		# parser.add_argument('--log-level', aliases=['-ll'])
		if '-ll' in sys.argv:
			try:
				level = sys.argv[sys.argv.index('-ll')+1]
				self.set_listen_level(level)
			except IndexError:
				self.error('Logger was not able to be initialized to specified level. -ll Must be followed by Level Code.')
		elif '--log-level' in sys.argv:
			try:
				level = sys.argv[sys.argv.index('--log-level')+1]
				self.set_listen_level(level)
			except IndexError:
				self.error('Logger was not able to be initialized to specified level. --log-level Must be followed by Level Code.')
