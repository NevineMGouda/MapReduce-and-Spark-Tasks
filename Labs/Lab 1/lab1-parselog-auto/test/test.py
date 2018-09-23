import unittest
from solution import *


class Lab1(unittest.TestCase):
    def testParseLogALL(self):
        restuls = parseLog(LOG_FILE)
        answer =  [('199.72.81.55', 'GET', '/history/apollo/', 6245),
                ('unicomp6.unicomp.net', 'GET', '/shuttle/countdown/', 3985),
                ('199.120.110.21',
                'GET',
        '/shuttle/missions/sts-73/mission-sts-73.html',
          4085),
         ('burger.letters.com', 'GET', '/shuttle/countdown/liftoff.html', 0),
         ('199.120.110.21',
          'GET',
          '/shuttle/missions/sts-73/sts-73-patch-small.gif',
          4179),
         ('burger.letters.com', 'GET', '/images/NASA-logosmall.gif', 0),
         ('burger.letters.com', 'GET', '/shuttle/countdown/video/livevideo.gif', 0),
         ('205.212.115.106', 'GET', '/shuttle/countdown/countdown.html', 3985),
         ('d104.aa.net', 'GET', '/shuttle/countdown/', 3985),
         ('129.94.144.152', 'GET', '/', 7074)]

        self.assertEqual(restuls, answer)
