import os, sys
from   functools    import partial
from   subprocess   import Popen
from   urllib.parse import urlparse

stderr = partial(print, file=sys.stderr)


def download_db(server):
    args = ['--{}={}'.format(k, v) for k, v in {
        'host': server.hostname,
        'port': server.port,
        'username': server.username,
        'dbname': server.path.lstrip('/')
    }.items()]
    os.environ['PGPASSWORD'] = server.password
    Popen(['pg_dump', '-Fc', '--no-acl', '--no-owner'] + args,
          stdout=sys.stdout,
          stderr=sys.stderr).communicate()


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
        


