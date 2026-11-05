import contextlib
import socketserver
import threading
import unittest

from memcache import Client


@contextlib.contextmanager
def stats_server(items):
    commands = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            for response in (items, b'STAT version test-server\r\nEND\r\n'):
                command = self.rfile.readline()
                if not command:
                    return
                commands.append(command)
                if response is None:
                    return
                # Exercise readline buffering across multiple writes.
                self.wfile.write(response[:5])
                self.wfile.flush()
                self.wfile.write(response[5:])
                self.wfile.flush()

    with socketserver.TCPServer(('127.0.0.1', 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={'poll_interval': 0.01})
        thread.start()
        client = Client(['%s:%s' % server.server_address])
        try:
            yield client, commands
        finally:
            client.disconnect_all()
            server.shutdown()
            thread.join(timeout=2)
            assert not thread.is_alive()


class TestSlabStats(unittest.TestCase):
    def test_get_slabs_decodes_items_and_consumes_end(self):
        response = (b'STAT items:1:number 2\r\n'
                    b'STAT items:1:age 11\r\n'
                    b'STAT items:4:number 1\r\nEND\r\n')
        with stats_server(response) as (client, commands):
            stats = client.get_slabs()
            self.assertEqual(len(stats), 1)
            self.assertEqual(stats[0][1], {
                '1': {'number': '2', 'age': '11'},
                '4': {'number': '1'},
            })
            self.assertEqual(client.get_stats()[0][1],
                             {'version': 'test-server'})
            self.assertEqual(commands, [b'stats items\r\n', b'stats\r\n'])

    def test_get_slabs_accepts_empty_items(self):
        with stats_server(b'END\r\n') as (client, commands):
            self.assertEqual(client.get_slabs()[0][1], {})
            self.assertEqual(client.get_stats()[0][1],
                             {'version': 'test-server'})
            self.assertEqual(commands, [b'stats items\r\n', b'stats\r\n'])

    def test_get_slabs_preserves_connection_close(self):
        with stats_server(None) as (client, commands):
            self.assertEqual(client.get_slabs()[0][1], {})
            self.assertEqual(client.get_stats(), [])
            self.assertEqual(commands, [b'stats items\r\n'])

    def test_get_slabs_skips_unreachable_server(self):
        with stats_server(b'END\r\n') as (client, commands):
            client.servers[0].mark_dead('test')
            self.assertEqual(client.get_slabs(), [])
            self.assertEqual(commands, [])
