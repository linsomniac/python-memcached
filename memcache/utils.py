import six


def encode_command(cmd, key, headers, noreply, *args):
    cmd_bytes = cmd.encode('utf-8') if six.PY3 else cmd
    fullcmd = [cmd_bytes, b' ', key]

    if headers:
        if six.PY3:
            headers = headers.encode('utf-8')
        fullcmd.append(b' ')
        fullcmd.append(headers)

    if noreply:
        fullcmd.append(b' noreply')

    if args:
        fullcmd.append(b' ')
        fullcmd.extend(args)
    return b''.join(fullcmd)


def encode_key(key):
    if isinstance(key, tuple):
        if isinstance(key[1], six.text_type):
            return (key[0], key[1].encode('utf8'))
    elif isinstance(key, six.text_type):
        return key.encode('utf8')
    return key
