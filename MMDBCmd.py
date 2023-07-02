import argparse
import maxminddb
import copy
import pandas as pd
from ipaddress import IPv4Network, IPv6Network

def iterable(self):
    if self._metadata.ip_version == 4:
        start_node = self._start_node(32)
        start_network = IPv4Network((0, 0))
    else:
        start_node = self._start_node(128)
        start_network = IPv6Network((0, 0))

    search_nodes = [(start_node, start_network)]
    while search_nodes:
        node, network = search_nodes.pop()

        if network.version == 6:
            naddr = network.network_address
            if naddr.ipv4_mapped or naddr.sixtofour:
                # skip IPv4-Mapped IPv6 and 6to4 mapped addresses, as these are
                # already included in the IPv4 part of the tree below
                continue
            elif int(naddr) < 2 ** 32 and network.prefixlen == 96:
                # once in the IPv4 part of the tree, switch to IPv4Network
                ipnum = int(naddr)
                mask = network.prefixlen - 128 + 32
                network = IPv4Network((ipnum, mask))
        
        subnets = list(network.subnets())
        for bit in (0, 1):
            next_node = self._read_node(node, bit)
            subnet = subnets[bit]

            if next_node > self._metadata.node_count:
                data = self._resolve_data_pointer(next_node)
                yield (subnet, data)
            elif next_node < self._metadata.node_count:
                search_nodes.append((next_node, subnet))

parser = argparse.ArgumentParser(description='Process IP address data', add_help=False)
parser.add_argument('-f', '--file', type=str, help='File to process.', required=True)
parser.add_argument('--csv', type=str, help='Directory to save CSV formatted results to.', required=True)
parser.add_argument('--csvf', type=str, help='File name to save CSV formatted results to.', required=True)
parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS,
                    help='Show help and usage information')

args = parser.parse_args()

input_file = args.file
output_path = args.csv
output_file_name = args.csvf

counter = 0
write_header = True
row_format = {
    'range': "",
    'continent_code': "",
    'continent': "",
    'country_code': "",
    'country': "",
    'city': "",
    'region': "",
    'region_code': "",
    'latitude': None,
    'longitude': None,
    'location_accuracy_radius': None,
}

output_file = f'{output_path}/{output_file_name}'

with maxminddb.open_database(input_file) as reader:
    rows = []
    for node in iterable(reader):
        row = copy.deepcopy(row_format)
        row['range'] = format(node[0])
        d = node[1]

        if 'continent' in d:
            if 'code' in d['continent']:
                row['continent_code'] = d['continent']['code']
            if 'names' in d['continent']:
                if 'en' in d['continent']['names']:
                    row['continent'] = d['continent']['names']['en']
        
        if 'registered_country' in d:
            if 'iso_code' in d['registered_country']:
                row['country_code'] = d['registered_country']['iso_code']
            if 'names' in d['registered_country']:
                if 'en' in d['registered_country']['names']:
                    row['country'] = d['registered_country']['names']['en']
        
        if 'city' in d:
            if 'names' in d['city']:
                if 'en' in d['city']['names']:
                    row['city'] = d['city']['names']['en']

        if 'subdivisions' in d:
            if 'names' in d['subdivisions'][0]:
                if 'en' in d['subdivisions'][0]['names']:
                    row['region'] = d['subdivisions'][0]['names']['en']
            if 'names' in d['subdivisions'][0]:
                row['region_code'] = d['subdivisions'][0]['iso_code']

        if 'location' in d:
            if 'latitude' in d['location']:
                row['latitude'] = d['location']['latitude']
            if 'longitude' in d['location']:
                row['longitude'] = d['location']['longitude']
            if 'accuracy_radius' in d['location']:
                row['location_accuracy_radius'] = d['location']['accuracy_radius']

        if 'longitude' in d:
            row['longitude'] = d['longitude']
        
        counter += 1
        rows.append(row)

        if counter % 10000 == 0:
            pd.DataFrame(rows).to_csv(output_file, mode='a', header=write_header, index=False)
            write_header = False
            rows = []
            print('.', end='')

        if counter % 1000000 == 0:
            print('.')

    # Write remaining rows
    if rows:
        pd.DataFrame(rows).to_csv(output_file, mode='a', header=write_header, index=False)
