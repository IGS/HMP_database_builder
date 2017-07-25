#!/usr/bin/env python

import argparse,os,subprocess,errno,datetime

def main():

    parser = argparse.ArgumentParser(description='Script to build a Neo4j database using OSDF.')
    parser.add_argument('--http', '-hp', type=str, help='Port to map the http port to, should not be 7474 to avoid conflict of live database.')
    parser.add_argument('--bolt', '-bp', type=str, help='Port to map the bolt port to, should not be 7687 to avoid conflict of live database.')
    parser.add_argument('--tmp_dir', '-td', type=str, help='Temporary location to build and place the loaded Neo4j database. MUST be an absolute path to mount via Docker.')
    parser.add_argument('--neo4j_exe', '-ne', type=str, help='Location of the Neo4j executable.')
    parser.add_argument('--neo4j_version', '-nv', type=str, help='Version of Neo4j to load to (e.g. 3.1.1).')
    parser.add_argument('--neo4j_db_path', '-ndp', type=str, help='Path to ~/data/databases to move the newly made Neo4j database to.')
    parser.add_argument('--batch_size', '-bs', type=int, help='How many Cypher transactions to commit in each batch via py2neo.')
    parser.add_argument('--db', '-d', type=str, help='URL:PORT for CouchDB of OSDF.')
    parser.add_argument('--loader_script', '-ls', type=str, help='Location of couchdb2neo4j_with_tags.py or other loader script.')
    args = parser.parse_args()

    try: # Build a tmp directory to mount the transient Neo4j database
        os.makedirs(args.tmp_dir)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    start_neo4j_docker = "docker run --name transient_neo4j --publish={0}:7474 --publish={1}:7687 --env=NEO4J_AUTH=none --volume={2}:/data neo4j:{3}".format(args.http,args.bolt,args.tmp_dir,args.neo4j_version)
    neo4j = subprocess.Popen(start_neo4j_docker.split(),stdout=subprocess.PIPE)

    for line in iter(neo4j.stdout.readline,''):
        # Wait until Neo4j is exposed on 7474 and then we're good to go
        if '7474' in line: 
            break

    load_database = "{0} --http_port {1} --bolt_port {2} --batch_size {3} --db {4}".format(args.loader_script,args.http,args.bolt,args.batch_size,args.db)
    subprocess.call(load_database.split())
    neo4j.kill()

    stop_neo4j_docker = "docker rm -f transient_neo4j"
    subprocess.call(stop_neo4j_docker.split())

    stop_neo4j = "{0} stop".format(args.neo4j_exe)
    subprocess.call(stop_neo4j.split())

    archive_old_database = "tar czf {0}/{1}.tar.gz {0}/graph.db".format(args.neo4j_db_path,datetime.date.today())
    subprocess.call(archive_old_database.split())

    remove_old_database = "rm -rf {0}/graph.db".format(args.neo4j_db_path)
    subprocess.call(remove_old_database.split())

    move_new_database = "mv {0}/databases/graph.db {1}/".format(args.tmp_dir,args.neo4j_db_path)
    subprocess.call(move_new_database.split())

    start_neo4j = "{0} start".format(args.neo4j_exe)
    subprocess.call(start_neo4j.split())
    
    remove_tmp_dir = "rm -rf {0}".format(args.tmp_dir) # -rf due to Neo4j data/dbms
    subprocess.call(remove_tmp_dir)


if __name__ == '__main__':
    main()
