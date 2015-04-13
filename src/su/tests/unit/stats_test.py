import unittest

from su import stats

class TimingStatBufferTest(unittest.TestCase):
    def test_tsb(self):
        tsb = stats.TimingStatBuffer()
        self.assertEqual([], list(tsb.flush()))

        for i in range(1, 4):
            for j in range(i):
                tsb.record(str(i), 0, 0.1 * (j + 1))
        self.assertEqual(
            {('1', '1|c'),
             ('1', '100.0|ms'),
             ('2', '2|c'),
             ('2', '150.0|ms'),  # (0.1 + 0.2) / 2
             ('3', '3|c'),
             ('3', '200.0|ms')}, # (0.1 + 0.2 + 0.3) / 3
            set(tsb.flush()))

class CountingStatBufferTest(unittest.TestCase):
    def test_csb(self):
        csb = stats.CountingStatBuffer()
        self.assertEqual([], list(csb.flush()))

        for i in range(1, 4):
            for j in range(i):
                csb.record(str(i), j + 1)
        self.assertEqual(
            {('1', '1|c'),
             ('2', '3|c'),
             ('3', '6|c')},
            set(csb.flush()))

class StringCountBufferTest(unittest.TestCase):
    def test_encode_string(self):
        enc = stats.StringCountBuffer._encode_string
        self.assertEqual('test', enc('test'))
        self.assertEqual('\\n\\&\\\\&', enc('\n|\\&'))

    def test_scb(self):
        scb = stats.StringCountBuffer()
        self.assertEqual([], list(scb.flush()))

        for i in range(1, 4):
            for j in range(i):
                for k in range(j + 1):
                    scb.record(str(i), str(j))
        self.assertEqual(
            {('1', '1|s|0'),
             ('2', '1|s|0'),
             ('2', '2|s|1'),
             ('3', '1|s|0'),
             ('3', '2|s|1'),
             ('3', '3|s|2')},
            set(scb.flush()))

class FakeUdpSocket:
    def __init__(self, *ignored_args):
        self.host = None
        self.port = None
        self.datagrams = []

    def sendto(self, datagram, host_port):
        self.datagrams.append(datagram)

class StatsdConnectionUnderTest(stats.StatsdConnection):
    _make_socket = FakeUdpSocket

class StatsdConnectionTest(unittest.TestCase):
    @staticmethod
    def connect(compress=False):
         return StatsdConnectionUnderTest('host:1000', compress=compress)

    def test_parse_addr(self):
        self.assertEqual(
            ('1:2', 3), stats.StatsdConnection._parse_addr('1:2:3'))

    def test_send(self):
        conn = self.connect()
        conn.send((i, i) for i in range(1, 6))
        self.assertEqual(
            [b'1:1\n2:2\n3:3\n4:4\n5:5'],
            conn.sock.datagrams)

        # verify compression
        data = [('a.b.c.w', 1), ('a.b.c.x', 2), ('a.b.c.y', 3), ('a.b.z', 4),
                ('bbb', 5), ('bbc', 6)]
        conn = self.connect(compress=True)
        conn.send(reversed(data))
        self.assertEqual(
            [b'a.b.c.w:1\n^06x:2\n^06y:3\n^04z:4\nbbb:5\nbbc:6'],
            conn.sock.datagrams)
        conn = self.connect(compress=False)
        conn.send(reversed(data))
        self.assertEqual(
            [b'bbc:6\nbbb:5\na.b.z:4\na.b.c.y:3\na.b.c.x:2\na.b.c.w:1'],
            conn.sock.datagrams)

        # ensure send is a no-op when not connected
        conn.sock = None
        conn.send((i, i) for i in range(1, 6))

class StatsdClientUnderTest(stats.StatsdClient):
    @classmethod
    def _data_iterator(cls, x):
       return sorted(iter(x))

    @classmethod
    def _make_conn(cls, addr):
        return StatsdConnectionUnderTest(addr, compress=False)

class StatsdClientTest(unittest.TestCase):
    def test_flush(self):
        client = StatsdClientUnderTest('host:1000')
        client.timing_stats.record('t', 0, 1)
        client.counting_stats.record('c', 1)
        client.flush()
        self.assertEqual(
            [b'c:1|c\nt:1000.0|ms\nt:1|c'],
            client.conn.sock.datagrams)

class CounterAndTimerTest(unittest.TestCase):
    @staticmethod
    def client():
        return StatsdClientUnderTest('host:1000')

    def test_get_stat_name(self):
        self.assertEqual(
            'a.b.c',
            stats._get_stat_name('a', '', u'b', None, 'c', 0))

    def test_counter(self):
        c = stats.Counter(self.client(), 'c')
        c.increment('a')
        c.increment('b', 2)
        c.decrement('c')
        c.decrement('d', 2)
        c += 1
        c -= 2
        self.assertEqual(
            {('c.a', '1|c'),
             ('c.b', '2|c'),
             ('c.c', '-1|c'),
             ('c.d', '-2|c'),
             ('c', '-1|c')},
            set(c.client.counting_stats.flush()))
        self.assertEqual(set(), set(c.client.counting_stats.flush()))

    def test_timer(self):
        t = stats.Timer(self.client(), 't')
        times = iter(i / 10.0 for i in range(10))
        t._time = lambda : next(times)
        self.assertRaises(AssertionError, t.intermediate, 'fail')
        self.assertRaises(AssertionError, t.stop)

        t.start()
        t.intermediate('a')
        t.intermediate('b')
        t.intermediate('c')
        t.stop(subname='t')

        self.assertRaises(AssertionError, t.intermediate, 'fail')
        self.assertRaises(AssertionError, t.stop)
        t.send('x', 0, 0.5)

        self.assertEqual(
            {('t.a', '1|c'),
             ('t.a', '100.0|ms'),
             ('t.b', '1|c'),
             ('t.b', '100.0|ms'),
             ('t.c', '1|c'),
             ('t.c', '100.0|ms'),
             ('t.t', '1|c'),
             ('t.t', '400.0|ms'),
             ('t.x', '1|c'),
             ('t.x', '500.0|ms')},
            set(t.client.timing_stats.flush()))
        self.assertEqual(set(), set(t.client.timing_stats.flush()))
