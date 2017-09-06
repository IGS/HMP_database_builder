#!/usr/bin/env python3

# Script to isolate all the possible metadata values for every key. This is 
# specific to the *attribute nodes as these have more freeform inputs than
# the others. Before checking a file, will want to subset by just the *attr 
# nodes using a command like:
# 
# grep '"node_type":"[a-z]\{3,\}\_attr*"' couchdb_changesfeed.json > filtered_dump.json
#
# ./HMP_database_builder/inspect_metadata.py -if filtered_dump.json -of all_persistent_vals.out
#
# Can transfer all these newly found fields in mass to accs_for_couchdb2neo4j via (then replace @@ with '):
# cut -f1 test3 | sort | awk '{ print "\@\@"$0"\@\@,"}'
# 
# Author: James Matsumura
# Contact: jmatsumura@som.umaryland.edu

import argparse,json,re
from collections import defaultdict
from accs_for_couchdb2neo4j import meta_null_vals, keys_to_keep

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
                    unnested_kv_generator(json_line['doc']['meta'],'meta',all_possible)

    with open(args.outfile,'w') as outf:
        for k,v in all_possible.items():
            outf.write("{}\t{}\n".format(k,v))

def unnested_kv_generator(json_input,key,uniq_vals):

    if isinstance(json_input, dict):
        if key in keys_to_keep:
            for k,v in json_input.items():
                unnested_kv_generator(json_input[k],"{}_{}".format(key,k),uniq_vals)
        else:
            for k in json_input:
                unnested_kv_generator(json_input[k],k,uniq_vals)
    elif isinstance(json_input, list):
        print(json_input)
        for item in json_input:
            unnested_kv_generator(item,key,uniq_vals)
    else:
        if isinstance(json_input,str):
            if json_input.lower() not in meta_null_vals:
                uniq_vals[key].add(json_input)
        else: # allow bools through
            uniq_vals[key].add(json_input)

if __name__ == '__main__':
    main()
