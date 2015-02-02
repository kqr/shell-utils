import os, re, sys
from   collections  import namedtuple
from   functools    import partial
from   itertools    import chain
from   subprocess   import Popen, PIPE
from   urllib.parse import urlparse

stderr = partial(print, file=sys.stderr)
Postgres = namedtuple('Postgres',
    ['host', 'port', 'username', 'password', 'dbname'])


def get_pg_args(server):
    if server.password:
        os.environ['PGPASSWORD'] = server.password

    return ('--{}={}'.format(k, v) for k, v in {
        'host': server.host,
        'port': server.port,
        'username': server.username,
        'dbname': server.dbname
    }.items() if v is not None)


def create_db(dbname):
    create = Popen(['createdb', dbname], stderr=PIPE)
    res = create.stderr.read().decode('utf-8')
    if re.search(r'ERROR:  database "{}" already exists'.format(dbname), res):
        confirmation = input('Database "{}" already exists and will be '
                             'replaced. Continue? (y/N) '.format(dbname))
        if confirmation not in ['y', 'Y']:
            sys.exit()


def dump_db(server):
    dump_cmd = ('pg_dump','-Fc','--no-acl','--no-owner')
    return Popen(chain(dump_cmd, get_pg_args(server)),
                 stdout=PIPE, stderr=sys.stderr)


def restore_db(server, data, warn_nonempty):
    if warn_nonempty:
        create_db(server.dbname)

    restore_cmd = ('pg_restore','--verbose','--clean','--no-acl','--no-owner')
    return Popen(chain(restore_cmd, get_pg_args(server)),
                 stdin=data, stdout=sys.stdout, stderr=sys.stderr)


def sync_db(from_db, to_db, warn_nonempty=True):
    data = dump_db(from_db).stdout
    restore_db(to_db, data, warn_nonempty).wait()


if __name__ == '__main__':
    try:
        parsed = urlparse(sys.argv[2])
    except IndexError:
        stderr('Usage: python3 {} <DOWN/UP> <connection string>'.format(sys.argv[0]))
        sys.exit(1)

    if parsed.scheme != 'postgres':
        stderr('The protocol specified in the connection string must be postgres!')
        sys.exit(1)

    remote = Postgres(
        host=parsed.hostname,
        port=parsed.port,
        username=parsed.username,
        password=parsed.password,
        dbname=parsed.path.lstrip('/')
    )
    local = Postgres(
        host='localhost',
        port=None,
        username=os.environ['USER'],
        password=None,
        dbname=remote.dbname
    )

    if sys.argv[1] == 'DOWN':
        sync_db(from_db=remote, to_db=local)
        sys.exit()

    elif sys.argv[1] == 'UP':
        sync_db(from_db=local, to_db=remote, warn_nonempty=False)
        sys.exit()

    else:
        stderr('Usage: python3 {} <DOWN/UP> <connection string>'.format(sys.argv[0]))
        sys.exit(1)
        


