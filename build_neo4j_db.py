#!/usr/bin/env python

import argparse,os,subprocess,errno,time

def main():

    parser = argparse.ArgumentParser(description='Script to build a Neo4j database using OSDF.')
    parser.add_argument('--http', '-hp', type=str, help='Port to map the http port to, should not be 7474 to avoid conflict of live database.')
    parser.add_argument('--bolt', '-bp', type=str, help='Port to map the bolt port to, should not be 7687 to avoid conflict of live database.')
    parser.add_argument('--tmp_dir', '-td', type=str, help='Temporary location to build and place the loaded Neo4j database.')
    args = parser.parse_args()

    # Build a tmp directory to mount the transient Neo4j database
    try:
        os.makedirs(args.tmp_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    start_neo4j_docker = "docker run --name transient_neo4j --publish={0}:7474 --publish={1}:7687 --volume={2}:/data neo4j:3.1.3".format(args.http,args.bolt,args.tmp_dir)
    neo4j = subprocess.Popen(start_neo4j_docker.split())
    time.sleep(30)
    neo4j.kill()

    # Cleanup after done loading/moving. Remove docker container and directory
    stop_neo4j_docker = "docker rm -f transient_neo4j"
    subprocess.call(stop_neo4j_docker.split())
    
    try:
        os.rmdir(args.tmp_dir)
    except OSError as exception:
        raise


if __name__ == '__main__':
    main()