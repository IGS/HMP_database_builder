#!/usr/bin/env python3

# Script to export the user, session, and query nodes from a HMP Neo4j 
# instance. Will generate an output file that can then be fed into the
# corresponding neo4j_import_user_info.py. 
#
# Author: James Matsumura
# Contact: jmatsumura@som.umaryland.edu

# ../HMP_database_builder/inspect_metadata.py -if no_carots.json 

import argparse,json,re
from collections import defaultdict

def main():

    parser = argparse.ArgumentParser(description='Script to isolate all the unique values inserted into attribute nodes.')
    parser.add_argument('--metadata_file', '-mf', type=str, help='Name of a CouchDB dump to inspect nested metadata in.')
    parser.add_argument('--outfile', '-of', type=str, help='Name of an outfile to write with the metadata values.')
    args = parser.parse_args()

    all_possible = defaultdict(set)

    with open(args.metadata_file,'r') as inf:
        for line in inf:

            line = line.strip()
            line = line.strip(',')
            json_line = json.loads(line)

            if 'doc' in json_line:
                if 'meta' in json_line['doc']:
                    if re.search(r'\_attr*',json_line['doc']['node_type']):
                        unnested_kv_generator(json_line['doc']['meta'],'meta',all_possible)

    with open(args.outfile,'w') as outf:
        for k,v in all_possible.items():
            outf.write("{}\t{}\n".format(k,v))
                

def unnested_kv_generator(json_input,key,uniq_vals):
    if isinstance(json_input, dict):
        for k in json_input:
            unnested_kv_generator(json_input[k],k,uniq_vals)
    elif isinstance(json_input, list):
        for item in json_input:
            unnested_kv_generator(item,key,uniq_vals)
    else:
        if json_input: # ignore blanks
            uniq_vals[key].add(json_input)

if __name__ == '__main__':
    main()
