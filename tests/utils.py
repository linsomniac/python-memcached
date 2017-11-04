from contextlib import contextmanager
import sys

from six import StringIO


@contextmanager
def captured_output(stream_name):
    """Return a context manager used by captured_stdout/stdin/stderr
    that temporarily replaces the sys stream *stream_name* with a StringIO.

    This function and the following ``captured_std*`` are copied
    from CPython's ``test.support`` module.
    """
    orig_stdout = getattr(sys, stream_name)
    setattr(sys, stream_name, StringIO())
    try:
        yield getattr(sys, stream_name)
    finally:
        setattr(sys, stream_name, orig_stdout)


def captured_stderr():
    """Capture the output of sys.stderr:

       with captured_stderr() as stderr:
           print('hello', file=sys.stderr)
       self.assertEqual(stderr.getvalue(), 'hello\n')
    """
    return captured_output('stderr')
