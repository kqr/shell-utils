from   functools   import partial
import itertools
import os, sys, re
from   subprocess  import check_output, CalledProcessError

shell  = partial(check_output, shell=True)
stderr = partial(print, file=sys.stderr)

class Snapshot:
    describe_cmd = 'ec2-describe-snapshots --region eu-west-1'
    create_cmd   = 'ec2-create-snapshot --region eu-west-1'
    delete_cmd   = 'ec2-delete-snapshot --region eu-west-1'

    def __init__(self, machine_name, volume_id, kind):
        self.machine_name = machine_name
        self.volume_id = volume_id
        self.kind = kind

        args = (self.machine_name, self.kind)
        self.name_pattern = re.compile(r'{}_backup_{}_(?P<number>\d+)'.format(*args))
        self.snapshot_pattern = re.compile(r'snap-[0-9a-f]+')

    def make_name(self, number):
        return '{}_backup_{}_{}'.format(self.machine_name, self.kind, number)

    def find(self):
        for volume in shell(Snapshot.describe_cmd).decode('utf-8').split('\n'):
            m = self.name_pattern.search(volume)
            snapshot_id = self.snapshot_pattern.search(volume)
            if m and self.volume_id in volume:
                yield int(m.group('number')), snapshot_id.group(0)

    def create(self, name):
        shell('{} -d {} {}'.format(Snapshot.create_cmd, name, self.volume_id))

    def delete(self, name):
        shell('{} {}'.format(Snapshot.delete_cmd, name))


def main(snapshot):
    try:
        snapshots = sorted(list(snapshot.find()), key=lambda a: a[0])
    except CalledProcessError:
        args = (snapshots.kind, snapshots.volume_id, snapshots.machine_name)
        desc = '{} snapshots for {} on {}'.format(*args)
        stderr('Could not list available {}, aborting...'.format(desc))
        sys.exit(1)

    try: next = snapshot.make_name(snapshots[-1][0] + 1)
    except IndexError: next = snapshot.make_name(0)

    try:
        snapshot.create(next)
    except CalledProcessError:
        stderr('Failed to create snapshot {}, aborting...'.format(next))
        sys.exit(1)

    try: remove = snapshots[-4][1]
    except IndexError: return

    try:
        snapshot.delete(remove)
    except CalledProcessError:
        stderr('Failed to delete snapshot {}'.format(remove))


if __name__ == '__main__':
    machine_name, kind, volume_id = sys.argv[1:]
    main(Snapshot(machine_name, volume_id, kind))