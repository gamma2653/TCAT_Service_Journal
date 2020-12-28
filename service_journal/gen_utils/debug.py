import sys
import argparse
import traceback as tb
from datetime import datetime
from enum import IntEnum
class Level(IntEnum):
	'''
	Higher levels denote finer log messages. Lower numbers (down to 0) denote
	more fatal errors.
	'''
	FATAL = 0
	ERROR = 1
	WARN = 2
	INFO = 3
	FINE = 4
	FINER = 5
	FINEST = 6
	def __str__(self):
		return self.name

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
	def __init__(self, module, level=Level.INFO, gen_file=True):
		self.level = level
		self.module = module
		self.gen_file = gen_file
	def set_listen_level(self, level):
		'''
		Accepts level as a str, enum, or int, then sets internal Logger.level as
		the appropriate Level. The Logger will from that point forward only log
		when the level is lower than or equal to the given level (as or more fatal).
		'''
		try:
			try:
				level = Level(int(level))
				self.level = level
			except ValueError:
				level = Level[str(level)]
				self.level = level
			except:
				self.error(f'Ran into an unexpected exception while trying to set level:\n {tb.format_exc(limit = 10)}'
		except:
			self.error(f'Ran into an exception while trying to set level:\n {tb.format_exc(limit = 10)}'
	def log(self, message, level):
		'''
		Logs the message if it is the given level is lower than or equal to the
		internal level (as or more fatal).
		'''
		if level <= self.level:
			log_message = f'[{datetime.now().strftime("%H:%M:%S")}] [{str(self.module)}]: [{level}] {message}'
			print(log_message)
			if self.gen_file:
				with open(f'{str(self.module)}.log', 'a') as f:
					f.write(log_message+'\n')

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


	def set_module(self, module):
		self.module = module

	def read_args(self):
		'''
		Called when a module wants to set log levels using arguments.
		'''
		parser = argparse.ArgumentParser()
		parser.add_argument('-ll', '--log-level', default=Level.INFO)
		args = parser.parse_args()
		self.set_listen_level(args.log_level)
