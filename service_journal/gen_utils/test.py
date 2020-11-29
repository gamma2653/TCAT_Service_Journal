from service_journal.gen_utils.debug import Logger, Level
from time import sleep
import unittest
import os

deflt_messages = ('This is a fatal.', 'This is an error.', 'This is a warn.',
  'This is an info.', 'This is a fine.', 'This is a finer.', 'This is a finest.')

def test_granularity(s, up_to):
    flag = True
    for level in Level:
        flag = ( flag and s.count(deflt_messages[level]) ) if ( level<=up_to ) else flag
    return bool(flag)

class TestMultipleLoggers(unittest.TestCase):
    def testFileDetectLevels(self):
        loggers = logger0, logger1, logger2, logger3 = Logger('Module0'), Logger('Module1'), Logger('Module2'), Logger('Module3')
        # for logger in loggers:
        #     logger.read_args() #No need, test case
        criterion = [Level.ERROR, Level.INFO, Level.WARN, Level.FINEST]
        for i, logger in enumerate(loggers):
            logger.set_listen_level(criterion[i])
        for level in Level:
            for logger in loggers:
                logger.log(deflt_messages[level], level)
            sleep(1)

        results = [True,True,True,True]
        for i, logger in enumerate(loggers):
            with open(f'Module{i}.log', 'r') as f:
                data = f.read()
                results[i]= test_granularity(data, criterion[i])
        for i in range(0,4):
            os.remove(f'Module{i}.log')
        for i in range(0,4):
            self.assertTrue(results[i])
    def testFileNotDetectLevels(self):
        loggers = logger0, logger1, logger2, logger3 = Logger('Module0'), Logger('Module1'), Logger('Module2'), Logger('Module3')
        # for logger in loggers:
        #     logger.read_args() #No need, test case
        criterion = [Level.ERROR, Level.INFO, Level.WARN, Level.FINER]
        for i, logger in enumerate(loggers):
            logger.set_listen_level(criterion[i])
        for i, level in enumerate(criterion):
            criterion[i] = Level( (criterion[i]+1) if (criterion[i]+1)<len(Level) else criterion[i] )
        for level in Level:
            for logger in loggers:
                logger.log(deflt_messages[level], level)
            sleep(1)

        results = [True,True,True,True]
        for i, logger in enumerate(loggers):
            with open(f'Module{i}.log', 'r') as f:
                data = f.read()
                results[i]= test_granularity(data, criterion[i])
        for i in range(0,4):
            os.remove(f'Module{i}.log')
        for i in range(0,4):
            self.assertFalse(results[i])

def run_tests():
    unittest.main()
if __name__ == '__main__':
    print('\n\nRunning test suite...\n\n')
    run_tests()
