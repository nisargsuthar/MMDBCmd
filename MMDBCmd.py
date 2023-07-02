import argparse
import maxminddb
import copy
import pandas as pd
import os
from ipaddress import IPv4Network, IPv6Network


def iterable(reader):
    if reader.metadata().ip_version == 4:
        start_node = reader._start_node(32)
        start_network = IPv4Network((0, 0))
    else:
        start_node = reader._start_node(128)
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
            next_node = reader._read_node(node, bit)
            subnet = subnets[bit]

            if next_node > reader.metadata().node_count:
                data = reader._resolve_data_pointer(next_node)
                yield (subnet, data)
            elif next_node < reader.metadata().node_count:
                search_nodes.append((next_node, subnet))


def process_file(input_file, output_path):
    # Get the directory path of the input_file
    file_directory = os.path.dirname(input_file)

    # Create the output directory path in the output_path directory
    output_directory = os.path.join(output_path, os.path.relpath(file_directory, start=input_directory))
    os.makedirs(output_directory, exist_ok=True)

    # Create the output file path
    output_file_name = os.path.splitext(os.path.basename(input_file))[0] + '.csv'
    output_file = os.path.join(output_directory, output_file_name)

    with maxminddb.open_database(input_file) as reader:
        rows = []
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


def process_directory(directory, output_path):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.mmdb'):
                input_file = os.path.join(root, file)
                process_file(input_file, output_path)


# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process IP address data', add_help=False)
parser.add_argument('-d', '--directory', type=str, help='Directory to process .mmdb files recursively.', required=True)
parser.add_argument('--csv', type=str, help='Directory to save CSV formatted results to.', required=True)
parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS,
                    help='Show help and usage information')

args = parser.parse_args()

input_directory = args.directory
output_directory = args.csv

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)

process_directory(input_directory, output_directory)
