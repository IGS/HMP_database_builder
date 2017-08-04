#!/usr/bin/env python

# Script to export the user, session, and query nodes from a HMP Neo4j 
# instance. Will generate an output file that can then be fed into the
# corresponding neo4j_import_user_info.py. 
#
# Author: James Matsumura
# Contact: jmatsumura@som.umaryland.edu

import argparse,json,time
from py2neo import Graph

def main():

    parser = argparse.ArgumentParser(description='Script to build a Neo4j database using OSDF.')
    parser.add_argument('--http_port', '-hp', type=int, help='Port to map the http port to, should not be 7474 to avoid conflict of live database.')
    parser.add_argument('--bolt_port', '-bp', type=int, help='Port to map the bolt port to, should not be 7687 to avoid conflict of live database.')
    parser.add_argument('--neo4j_password', '-np', type=str, help='Password for the Neo4j database')
    parser.add_argument('--outfile', '-o', type=str, help='Name of the outfile to generate.')
    args = parser.parse_args()

    cy = Graph(password = args.neo4j_password, bolt_port = args.bolt_port, http_port = args.http_port)

    extract_session_user_query_cypher = """
        MATCH (u:user)-[:saved_query]->(q:query) 
        WITH u,q 
        OPTIONAL MATCH (s:session)<-[:has_session]-(u) 
        RETURN s,u,q
    """

    # If there's a recent session, first run this query to establish that
    create_session_user_cypher = """
        MERGE (u:user {{ username:'{0}' }}) 
        MERGE (s:session {{ id:'{1}', created_at:{2} }}) 
        MERGE (u)-[:has_session]->(s)
    """

    # Regardless of if a session is present, attach a query to the user
    create_user_query_cypher = """
        MERGE (u:user {{ username:'{0}' }}) 
        MERGE (q:query {{ query_str:'{1}', url:'{2}', f_count{3}, s_count{4} }}) 
        MERGE (u)-[:saved_query]->(q)
    """

    # Each element in this list will be a unique query attached to a particular
    # user (and potentially a session).
    relevant_nodes = cy.run(extract_session_user_query_cypher).data()

    with open(args.outfile,'w') as out:
        for res in relevant_nodes:

            if 's' in res:
                cleansed_string = create_session_user_cypher.format(
                        res['u']['username'],
                        res['s']['id'],
                        res['s']['created_at']
                    ).strip().replace("\n"," ")

                out.write("{0}\n".format(cleansed_string))
            
            # no matter what, there's a query

            cleansed_string = create_user_query_cypher.format(
                    res['u']['username'],
                    res['q']['query_str'],
                    res['q']['url'],
                    res['q']['f_count'],
                    res['q']['s_count']
                ).strip().replace("\n"," ")

            out.write("{0}\n".format(cleansed_string))


if __name__ == '__main__':
    main()
