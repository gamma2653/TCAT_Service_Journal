from datetime import datetime
from enum import Enum
class Level(Enum):
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
	def log(message, level):
		if level <= self.level:
			print('[%s] [%s]: [%s] %s' % (datetime.now().strftime('%H:%M:%S'), level, self.module, message))
			# TODO: Add log.txt
	def finest(message):
		log(message, Level.FINEST)
	def finer(message):
		log(message, Level.FINER)
	def fine(message):
		log(message, Level.FINE)
	def info(message):
		log(message, Level.INFO)
	def warn(message):
		log(message, Level.WARN)
	def error(message):
		log(message, Level.ERROR)
	def fatal(message):
		log(message, Level.FATAL)

	def set_module(module):
		self.module = module
	def set_listen_level(level):
		self.level = level
