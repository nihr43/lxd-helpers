import os
import argparse
import random
from pylxd import Client
from functools import partial


# randomly distributes a group of instances withmatching name prefix across
# hardware.
def rebalance(client_factory, client, prefix):
    instances = client.instances.all()

    acting_set = []

    for i in instances:
        if i.name.startswith(prefix):
            acting_set.append(i)

    cluster = get_members(client)

    # repeatable distribution happens here.
    # we seed Random with the provided prefix:
    random.Random(prefix).shuffle(cluster)

    cursor = 0
    cursor_end = len(cluster)

    for i in acting_set:
        print('moving ' + i.name + ' to ' + cluster[cursor].server_name)

        target_client = client_factory(endpoint='https://' + cluster[cursor].server_name + ':8443')
        i.migrate(target_client, wait=True, live=True)
        cursor += 1
        if cursor >= cursor_end:
            cursor = 0


# returns list of cluster "members"
# members defined here: https://github.com/lxc/pylxd/blob/master/pylxd/models/cluster.py#L45
def get_members(client):
    cluster = client.cluster.get()
    nodes = cluster.members.all()
    return nodes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rebalance', help='Given a string prefix, rebalance matching instances accross cluster nodes')
    parser.add_argument('--drain', help='Drain a cluster node')

    args = parser.parse_args()

    home = os.environ['HOME']

    client_factory = partial(Client, cert=(home + '/.config/lxc/client.crt',
                                           home + '/.config/lxc/client.key'),
                             verify=home + '/.config/lxc/servercerts/lxd.local.crt')

    cluster_client = client_factory(endpoint='https://lxd.local:8443')

    if args.rebalance:
        rebalance(client_factory, cluster_client, args.rebalance)


if __name__ == '__main__':
    main()
