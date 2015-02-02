import os, re, sys
from   functools    import partial
from   itertools    import chain
from   subprocess   import Popen, PIPE
from   urllib.parse import urlparse

stderr = partial(print, file=sys.stderr)


def argsify(d):
  return ('--{}={}'.format(k, v) for k, v in d.items())


def download_db(server):
    dbname = server.path.lstrip('/')

    create = Popen(['createdb', dbname], stderr=PIPE)
    res = create.stderr.read().decode('utf-8')
    create.wait()
    if re.search(r'ERROR:  database "{}" already exists'.format(dbname), res):
        print('Database "{}" already exists and will be '
              'replaced. Continue? (y/N)'.format(dbname))
        confirmation = input()
        if confirmation not in ['y', 'Y']:
            return None

    os.environ['PGPASSWORD'] = server.password
    remote_args = argsify({
        'host': server.hostname,
        'port': server.port,
        'username': server.username,
        'dbname': dbname
    })
    dump = Popen(
        chain(['pg_dump', '-Fc', '--no-acl', '--no-owner'], remote_args),
        stdout=PIPE, stderr=sys.stderr
    )

    local_args = argsify({
        'host': 'localhost',
        'username': os.environ['USER'],
        'dbname': server.path.lstrip('/')
    })
    restore = Popen(
        chain(['pg_restore', '--verbose', '--clean', '--no-acl', '--no-owner'], local_args),
        stdin=dump.stdout, stdout=sys.stdout, stderr=sys.stderr
    )

    restore.wait()


if __name__ == '__main__':
    try:
        server = urlparse(sys.argv[2])
    except IndexError:
        stderr('Usage: python3 {} <DOWN/UP> <connection string>'.format(sys.argv[0]))
        sys.exit(1)

    if server.scheme != 'postgres':
        stderr('The protocol specified in the connection string must be postgres!')
        sys.exit(1)

    if sys.argv[1] == 'DOWN':
        download_db(server)
        sys.exit()

    elif sys.argv[1] == 'UP':
        raise NotImplementedError('Syncing up isn\'t implemented yet')
        sys.exit()

    else:
        stderr('Usage: python3 {} <DOWN/UP> <connection string>'.format(sys.argv[0]))
        sys.exit(1)
        


