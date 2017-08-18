#!/usr/bin/env python

# CREDIT TO necaris for the base script ~ https://gist.github.com/necaris/5604018
#
# Script to migrate OSDF CouchDB into Neo4j. This will collapse nodes into 
# subject, sample, file, and tag nodes. 
# 
# subject houses project, subject_attribute, and subject
#
# sample houses study, visit, visit_attribute, sample, and sample_attribute
#
# file is the file
#
# tag is the tag term attached to any of the nodes associated with a given
# file node. 
#
# derived_from edge houses the prep info
#
# The overall data structure looks like:
#
# (subject) <-[:extracted_from]- (sample) <-[:derived_from]- (file) -[:has_tag]-> (tag)
#
#-*-coding: utf-8-*-

import time,sys,argparse,requests
from py2neo import Graph
from accs_for_couchdb2neo4j import fma_free_body_site_dict, study_name_dict, file_format_dict

try:
    import simplejson as json
except ImportError:
    import json

def _print_error(message):
    """
    Print a message to stderr, with a newline.
    """
    sys.stderr.write(str(message) + "\n")
    sys.stderr.flush()


def _all_docs_by_page(db_url, page_size=10):
    """
    Helper function to request documents from CouchDB in batches ("pages") for
    efficiency, but present them as a stream.
    """
    # Tell CouchDB we only want a page worth of documents at a time, and that
    # we want the document content as well as the metadata
    view_arguments = {'limit': page_size, 'include_docs': "true"}

    # Keep track of the last key we've seen
    last_key = None

    while True:
        response = requests.get(db_url + "/_all_docs", params=view_arguments)

        # If there's been an error, stop looping
        if response.status_code != 200:
            _print_error("Error from DB: " + str(response.content))
            break

        # Parse the results as JSON. If there's an error, stop looping
        try:
            results = json.loads(response.content)
        except:
            _print_error("Unable to parse JSON: " + str(response.content))
            break

        # If there's no more data to read, stop looping
        if 'rows' not in results or not results['rows']:
            break

        # Otherwise, keep yielding results
        for r in results['rows']:
            last_key = r['key']
            yield r

        # Note that CouchDB requires keys to be encoded as JSON
        view_arguments.update(startkey=json.dumps(last_key), skip=1)

# All of these _build*_doc functions take in a particular "File" node (which)
# means anything below the "Prep" nodes and build a document containing all 
# the information along the particular path to get to that node. Each will
# have a new top of the structure called "File" where directly below this 
# location will contain all the relevant data for that particular node. 
# Everything else even with this level will be all the information contained
# at prep and above. This will result in a heavily DEnormalized dataset.
#
# The arguments are the entire set of nodes and the particular node that is the
# file representative. 

def _build_16s_raw_seq_set_doc(all_nodes_dict,node):

    doc = {}
    doc['main'] = node['doc']

    # If this is a pooled sample, build a different object that represents that state
    if type(doc['main']['linkage']['sequenced_from']) is list and len(set(doc['main']['linkage']['sequenced_from'])) > 1:
        doc['prep'] = _multi_find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['main']['linkage']['sequenced_from'])
        return _multi_collect_sample_through_project(all_nodes_dict,doc)

    else:
        doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['main']['linkage']['sequenced_from'])
        return _collect_sample_through_project(all_nodes_dict,doc)

def _build_16s_trimmed_seq_set_doc(all_nodes_dict,node):

    doc = {}
    doc['main'] = node['doc']

    if type(doc['main']['linkage']['computed_from']) is list and len(set(doc['main']['linkage']['computed_from'])) > 1:
        doc['16s_raw_seq_set'] = _multi_find_upstream_node(all_nodes_dict['16s_raw_seq_set'],'16s_raw_seq_set',doc['main']['linkage']['computed_from'])
        doc['prep'] = []
        for x in range(0,len(doc['16s_raw_seq_set'])):
            doc['prep'] += _multi_find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['16s_raw_seq_set'][x]['linkage']['sequenced_from'])
        doc['prep'] = {v['id']:v for v in doc['prep']}.values() # uniquifying
        doc['prep'] = _isolate_relevant_prep_edge(doc)

        if type(doc['prep']) is list:
            return _multi_collect_sample_through_project(all_nodes_dict,doc)

    else:
        doc['16s_raw_seq_set'] = _find_upstream_node(all_nodes_dict['16s_raw_seq_set'],'16s_raw_seq_set',doc['main']['linkage']['computed_from'])
        doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['16s_raw_seq_set']['linkage']['sequenced_from'])

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_abundance_matrix_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2)) # can be many here

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['computed_from'])

    # Notice that this IF precedes a second set of ELSE/IF statements, that is because
    # if this is an abundance_matrix derived from an abundance_matrix, we still build the
    # upstream structure in the same manner either way.
    if link in all_nodes_dict['abundance_matrix']:
        doc['abundance_matrix'] = _find_upstream_node(all_nodes_dict['abundance_matrix'],'abundance_matrix',link)
        # We now need to reset the link to be the other abundance_matrix
        link = _refine_link(doc['abundance_matrix']['linkage']['computed_from'])

    # process the middle pathway
    if link in all_nodes_dict['16s_trimmed_seq_set']:
        doc['16s_trimmed_seq_set'] = _find_upstream_node(all_nodes_dict['16s_trimmed_seq_set'],'16s_trimmed_seq_set',link)
        doc['16s_raw_seq_set'] = _find_upstream_node(all_nodes_dict['16s_raw_seq_set'],'16s_raw_seq_set',doc['16s_trimmed_seq_set']['linkage']['computed_from'])
        doc['prep'] = _find_upstream_node(all_nodes_dict['16s_dna_prep'],'16s_dna_prep',doc['16s_raw_seq_set']['linkage']['sequenced_from'])

    # process the left pathway
    elif (
        link in all_nodes_dict['microb_transcriptomics_raw_seq_set'] 
        or link in all_nodes_dict['host_transcriptomics_raw_seq_set'] 
        or link in all_nodes_dict['wgs_raw_seq_set'] 
        or link in all_nodes_dict['host_wgs_raw_seq_set'] 
        ):
        if link in all_nodes_dict['microb_transcriptomics_raw_seq_set']:
            which_upstream = 'microb_transcriptomics_raw_seq_set'
        elif link in all_nodes_dict['host_transcriptomics_raw_seq_set']:
            which_upstream = 'host_transcriptomics_raw_seq_set'
        elif link in all_nodes_dict['wgs_raw_seq_set']:
            which_upstream = 'wgs_raw_seq_set'
        elif link in all_nodes_dict['host_wgs_raw_seq_set']:
            which_upstream = 'host_wgs_raw_seq_set'

        doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
        link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

        if link in all_nodes_dict['wgs_dna_prep']:
            which_prep = 'wgs_dna_prep'
        elif link in all_nodes_dict['host_seq_prep']:
            which_prep = 'host_seq_prep'
        else:
            print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

        doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    # process the right pathway
    elif (
        link in all_nodes_dict['proteome']
        or link in all_nodes_dict['metabolome']
        or link in all_nodes_dict['lipidome']
        or link in all_nodes_dict['cytokine']
        ):
        if link in all_nodes_dict['proteome']:
            which_upstream = 'proteome'
        elif link in all_nodes_dict['metabolome']:
            which_upstream = 'metabolome'
        elif link in all_nodes_dict['lipidome']:
            which_upstream = 'lipidome'
        elif link in all_nodes_dict['cytokine']:
            which_upstream = 'cytokine'

        doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
        link = _refine_link(doc[which_upstream]['linkage']['derived_from'])

        if link in all_nodes_dict['microb_assay_prep']:
            which_prep = 'microb_assay_prep'
        elif link in all_nodes_dict['host_assay_prep']:
            which_prep = 'host_assay_prep'
        else:
            print("Made it here, so an ~ome node is missing an upstream ID of {0}.".format(link))

        doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_omes_doc(all_nodes_dict,node):

    doc = {}
    which_prep = "" # can be microb or host

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['derived_from'])

    if link in all_nodes_dict['microb_assay_prep']:
        which_prep = 'microb_assay_prep'
    elif link in all_nodes_dict['host_assay_prep']:
        which_prep = 'host_assay_prep'
    else:
        print("Made it here, so an ~ome node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_wgs_transcriptomics_doc(all_nodes_dict,node):

    doc = {}
    which_prep = "" # can be wgs_dna or host_seq

    doc['main'] = node['doc']

    if len(set(doc['main']['linkage']['sequenced_from'])) > 1:
        print(doc['main']['id'])

    link = _refine_link(doc['main']['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_wgs_assembled_or_viral_seq_set_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    # Assuming that WGS/HOST upstream nodes are never mixed, can identify using 
    # the first link which types the upstream and prep nodes are. 
    link = _refine_link(doc['main']['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])
    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    if type(doc['main']['linkage']['computed_from']) is list and len(set(doc['main']['linkage']['computed_from'])) > 1:
        doc[which_upstream] = _multi_find_upstream_node(all_nodes_dict[which_upstream],which_upstream,doc['main']['linkage']['computed_from'])
        doc['prep'] = []
        for x in range(0,len(doc[which_upstream])):
            doc['prep'] += _multi_find_upstream_node(all_nodes_dict[which_prep],which_prep,doc[which_upstream][x]['linkage']['sequenced_from'])
        doc['prep'] = {v['id']:v for v in doc['prep']}.values() # uniquifying
        doc['prep'] = _isolate_relevant_prep_edge(doc)
        if type(doc['prep']) is list:
            return _multi_collect_sample_through_project(all_nodes_dict,doc)

    else:
        doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)
        
    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_annotation_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    link = _refine_link(doc['main']['linkage']['computed_from'])

    if link in all_nodes_dict['viral_seq_set']:
        which_upstream = 'viral_seq_set'
    elif link in all_nodes_dict['wgs_assembled_seq_set']:
        which_upstream = 'wgs_assembled_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

def _build_clustered_seq_set_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc'] 

    doc['annotation'] = _find_upstream_node(all_nodes_dict['annotation'],'annotation',doc['main']['linkage']['computed_from'])
    link = _refine_link(doc['annotation']['linkage']['computed_from'])

    if link in all_nodes_dict['viral_seq_set']:
        which_upstream = 'viral_seq_set'
    elif link in all_nodes_dict['wgs_assembled_seq_set']:
        which_upstream = 'wgs_assembled_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'

    doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
    link = _refine_link(doc[which_upstream]['linkage']['sequenced_from'])

    if link in all_nodes_dict['wgs_dna_prep']:
        which_prep = 'wgs_dna_prep'
    elif link in all_nodes_dict['host_seq_prep']:
        which_prep = 'host_seq_prep'
    else:
        print("Made it here, so an WGS/HOST node is missing an upstream ID of {0}.".format(link))

    doc['prep'] = _find_upstream_node(all_nodes_dict[which_prep],which_prep,link)

    return _collect_sample_through_project(all_nodes_dict,doc)

# Function to traverse up from a trimmed seq set or WGS set through the raw
# edge links and find the singular relevant prep edge. This matches the 
# SRS tag attached to the 'main' node and matches it to the srs_id prop 
# in the prep node. 
def _isolate_relevant_prep_edge(doc):
    srs_tag = ""

    # grab the SRS ID from the tags attached to the file
    if 'tags' in doc['main']:
        for tag in doc['main']['tags']:
            if tag.startswith('SRS'):
                srs_tag = tag

    if srs_tag == "": # if found nothing in tags, check elsewhere
        if 'meta' in doc['main']:
            if 'assembly_name' in doc['main']['meta']:
                srs_tag = doc['main']['meta']['assembly_name']

    if srs_tag == "": # if found nothing in tags, check elsewhere
        if 'assembly_name' in doc['main']:
            srs_tag = doc['main']['assembly_name']

    # iterate over all the prep edges til you find the one
    for prep_edge in doc['prep']: # HMP I has 'srs_id'
        if 'srs_id' in prep_edge:
            if prep_edge['srs_id'] == srs_tag:
                return prep_edge
        elif 'tags' in prep_edge: # HMP II cases where SRS ID is in a tag
            for tag in prep_edge['tags']:
                if tag == srs_tag:
                    return prep_edge
        elif 'meta' in prep_edge:
            if 'srs_id' in prep_edge['meta']:
                if prep_edge['meta']['srs_id'] == srs_tag:
                    return prep_edge
            elif 'tags' in prep_edge['meta']:
                for tag in prep_edge['meta']['tags']:
                    if tag == srs_tag:
                        return prep_edge

    print("SRS# cannot be found upstream for ID: {0}".format(doc['main']['id']))
    return doc['prep'] # if we made it here, could not isolate upstream SRS

# This function takes in the dict of nodes from a particular node type, the name
# of this type of node, the ID specified by the linkage to isolate the node. 
# It returns the information of the particular upstream node. 
def _find_upstream_node(node_dict,node_name,link_id):
    
    # some test nodes have incorrect linkage styles.
    link_id = _refine_link(link_id)

    if link_id in node_dict:
        return node_dict[link_id]['doc']

    print("Made it here, so node type {0} with ID {1} is missing upstream.".format(node_name,link_id))

# This function collects sample-project nodes as these can consistently be 
# retrieved in a similar manner.
def _collect_sample_through_project(all_nodes_dict,doc):
    
    doc['sample'] = _find_upstream_node(all_nodes_dict['sample'],'sample',doc['prep']['linkage']['prepared_from'])
    doc['visit'] = _find_upstream_node(all_nodes_dict['visit'],'visit',doc['sample']['linkage']['collected_during'])
    doc['subject'] = _find_upstream_node(all_nodes_dict['subject'],'subject',doc['visit']['linkage']['by'])
    doc['study'] = _find_upstream_node(all_nodes_dict['study'],'study',doc['subject']['linkage']['participates_in'])
    doc['project'] = _find_upstream_node(all_nodes_dict['project'],'project',doc['study']['linkage']['part_of'])
    
    # Skip all the dummy data associated with the "Test Project"
    if doc['project']['id'] == '610a4911a5ca67de12cdc1e4b40018e1':
        return None
    else:
        doc = _append_attribute_data(all_nodes_dict,doc)
        return doc

# This function appends attribute data to the current node doc type.  
# Note this will only occur if there is such data available. Takes
# in all the nodes and the current doc to update
#
# Currently this ONLY does sample_attribute correctly as there is no live
# visit_attribute and subject_attribute with actual data. Likely the changes
# needed to accommodate the others are to:
# 1) add more to the sets 'meta_to_keep' and 'null_vals'
# 2) make sure that the relevant properties are one step down from the 'doc' key
def _append_attribute_data(all_nodes_dict,doc):

    attributes = ['sample_attribute','visit_attribute','subject_attribute']

    # properties which should persist from OSDF/cutlass to the portal browser
    meta_to_keep = {
        'fecalcal'
    }

    # values, chosen by submitters, to fill in blank fields if they have no 
    # data for the attribute
    null_vals = {
        'nan'
    }

    for attr in attributes:
        if attr in all_nodes_dict: # only act if attr data is present

            node_to_add_to = attr.split('_')[0]

            if type(doc[node_to_add_to]) is list:
                if doc[node_to_add_to][-1]['id'] in all_nodes_dict[attr]:
                    for k,v in all_nodes_dict[attr][doc[node_to_add_to][-1]['id']]['doc'].items():
                        if k in meta_to_keep:
                            if v not in null_vals:
                                # Slightly different since this is not a base node,
                                # be sure to keep consistent with what advanced query
                                # will mention this as (visit_metadata) even though
                                # it will be bound 1:1 with a sample. subject and 
                                # sample don't need a prefix since they're considered
                                # to be the root of the query and don't have a prefix
                                # to subset its data in autocomplete. 
                                if attr == 'visit_attribute':
                                    doc[node_to_add_to][-1]["visit_{0}".format(k)] = v
                                else:
                                    doc[node_to_add_to][-1][k] = v

            else:
                if doc[node_to_add_to]['id'] in all_nodes_dict[attr]:
                    for k,v in all_nodes_dict[attr][doc[node_to_add_to]['id']]['doc'].items():
                        if k in meta_to_keep:
                            if v not in null_vals:
                                if attr == 'visit_attribute': 
                                    doc[node_to_add_to]["visit_{0}".format(k)] = v
                                else:
                                    doc[node_to_add_to][k] = v

    return doc

# Similar to _find_upstream_node() except this one finds multiple upstream nodes.
# Returns a list at that dict for each upstream node. 
def _multi_find_upstream_node(node_dict,node_name,link_ids):
    
    link_list = list(set(link_ids))
    upstream_node_list = []

    for link_id in link_list:
        if link_id in node_dict:
            upstream_node_list.append(node_dict[link_id]['doc'])

    if len(upstream_node_list) == len(link_list):
        return upstream_node_list
    else:
        print("Made it here, so node type {0} doesn't have multiple upstream nodes as expected.".format(node_name))

# Similar to _collect_sample_through_project() except this works with many 
# upstream nodes. 
def _multi_collect_sample_through_project(all_nodes_dict,doc):

    # Establish each node type as a list to account for each different prep linkage
    init_nodes = ['sample','visit','subject','study','project']
    for nt in init_nodes:
        if nt not in doc: # needs to be handled here in the case of multiple files downstream of a prep
            doc[nt] = []

    # Maintain positions via list indices for each prep -> project path
    for x in range(0,len(doc['prep'])):

        doc['sample'].append(_find_upstream_node(all_nodes_dict['sample'],'sample',doc['prep'][x]['linkage']['prepared_from']))
        new_idx = (len(doc['sample'])-1) # occassionally this will be offset from prep if there's multiple downstream of prep
        doc['visit'].append(_find_upstream_node(all_nodes_dict['visit'],'visit',doc['sample'][new_idx]['linkage']['collected_during']))
        doc['subject'].append(_find_upstream_node(all_nodes_dict['subject'],'subject',doc['visit'][new_idx]['linkage']['by']))
        doc['study'].append(_find_upstream_node(all_nodes_dict['study'],'study',doc['subject'][new_idx]['linkage']['participates_in']))
        doc['project'].append(_find_upstream_node(all_nodes_dict['project'],'project',doc['study'][new_idx]['linkage']['part_of']))

        doc = _append_attribute_data(all_nodes_dict,doc)
    
    return doc

# This simply reformats a ID specified from a linkage to ensure it's a string 
# and not a list. Sometimes this happens when multiple linkages are noted but 
# it simply repeats pointing towards the same upstream node. Accepts a an entity
# following a linkage like doc['linkage']['sequenced_from'|'derived_from']
def _refine_link(linkage):

    if type(linkage) is list:
        if linkage[0] == '3a51534abc6e1a5ee6d9cc86c400a5a3': # don't consider the demo project a study, ignore this ID
            return linkage[1]
        else:
            return linkage[0]
    else:
        return linkage

# Build indexes for the three node types and their IDs that guarantee UNIQUEness
def _build_constraint_index(node,prop,cy):
    cstr = "CREATE CONSTRAINT ON (x:{0}) ASSERT x.{1} IS UNIQUE".format(node,prop)
    cy.run(cstr)

# Build indexes for searching on all the props that aren't ID. Takes which node
# to build all indexes on as well as the Neo4j connection.
def _build_all_indexes(node,cy):
    result = cy.run("MATCH (n:{0}) WITH DISTINCT keys(n) AS keys UNWIND keys AS keyslisting WITH DISTINCT keyslisting AS allfields RETURN allfields".format(node))
    for x in result:
        prop = x['allfields']
        if prop != 'id':
            cy.run("CREATE INDEX ON :{0}(`{1}`)".format(node,prop))

# Escape quotes to keep Cypher happy
def _mod_quotes(val):

    if isinstance(val, list):
        for x in val:
            if x in fma_free_body_site_dict:
                x = fma_free_body_site_dict[x]
            x = x.replace("'","\\'")
            x = x.replace('"','\\"')
            
    else:
        if val in fma_free_body_site_dict:
            val = fma_free_body_site_dict[val]
        val = val.replace("'","\\'")
        val = val.replace('"','\\"')

    return val

# Function to traverse the nested JSON documents from CouchDB and return
# a flattened set of properties specific to the particular node. The index
# value indicates whether or not this node has multiple upstream nodes. 
def _traverse_document(doc,focal_node,index):

    key_prefix = "" # for nodes embedded into other nodes, use this prefix to prepend their keys like project_name
    props = [] # list of all the properties to be added
    tags = [] # list of tags to be attached to the ID
    doc_id = "" # keep track of the ID for this particular doc.
    relevant_doc = "" # potentially reformat if being passed a doc with a list

    if focal_node not in ['subject','sample','main','prep']: # main is equivalent to file since a single doc represents a single file
        key_prefix = "{0}_".format(focal_node)

    if index == '':
        relevant_doc = doc[focal_node]
    else:
        relevant_doc = doc[focal_node][index]

    for key,val in relevant_doc.items():
        if key == 'linkage' or not val: # document itself contains all linkage info already
            continue

        if isinstance(val, int) or isinstance(val, float):
            key = key.encode('utf-8')
            props.append('`{0}{1}`:{2}'.format(key_prefix,key,val))
        elif isinstance(val, list): # lists should be urls, contacts, and tags
            for j in range(0,len(val)): 
                
                if key == 'tags':
                    tags.append(val)

                elif key == 'contact':
                    email = ""
                    for vals in val: # try find an email
                        if '@' in vals:
                            email = vals
                            break
                    if email:
                        props.append('`{0}contact`:"{1}"'.format(key_prefix,email))
                        break
                    else:
                        props.append('`{0}contact`:"{1}"'.format(key_prefix,val[j]))
                        break

                else:
                    endpoint = val[j].split(':')[0]
                    props.append('`{0}{1}`:"{2}"'.format(key_prefix,endpoint,val[j]))
        else:
            val = _mod_quotes(val)
            key = key.encode('utf-8')
            val = val.encode('utf-8')
            props.append('`{0}{1}`:"{2}"'.format(key_prefix,key,val))

        if key == "id":
            doc_id = val

    if focal_node == 'main': # missing file formats will default to text files (only true so far for lipidome)
        format_present = False
        for prop in props:
            if '`format`:' in prop:
                format_present = True
                break
        
        if not format_present:
            props.append('`format`:"Text"')

    props_str = (',').join(props)
    # Some formatting to get rid of empty key:value pairs
    props_str = props_str.replace('``:""','')
    props_str = props_str.replace(',,',',')

    if focal_node == 'main': # change syntax for file format and node_type
        for k,v in file_format_dict.items():
            props_str = props_str.replace('`format`:"{0}"'.format(k),'`format`:"{0}"'.format(v))

    return {'id':doc_id,'tag_list':tags,'prop_str':props_str}

def _add_unique_tags(th, tl):
    if isinstance(tl, basestring):
        if tl not in th:
            th[tl] = True
    else:
        for t in tl:
            _add_unique_tags(th, t)

# Takes in a list of Cypher statements and builds on it. The index value 
# differentiates a node with multiple upstream compared to one with single upstream.
def _generate_cypher(doc,index):

    cypher = []

    all_tags = {}

    file_info = _traverse_document(doc,'main','') # file is never a list, so never has an index
    props = "{0}".format(file_info['prop_str'])
    cypher.append("MERGE (node:file {{ {0} }})".format(props))

    sample_info = _traverse_document(doc,'sample',index)
    visit_info = _traverse_document(doc,'visit',index)
    study_info = _traverse_document(doc,'study',index)
    props = "{0},{1},{2}".format(sample_info['prop_str'],visit_info['prop_str'],study_info['prop_str'])
    cypher.append("MERGE (node:sample {{ {0} }})".format(props))

    subject_info = _traverse_document(doc,'subject',index)
    project_info = _traverse_document(doc,'project',index)
    props = "{0},{1}".format(subject_info['prop_str'],project_info['prop_str'])
    cypher.append("MERGE (node:subject {{ {0} }})".format(props))

    prep_info = _traverse_document(doc,'prep',index)

    cypher.append("MATCH (n1:subject{{id:'{0}'}}),(n2:sample{{id:'{1}'}}) MERGE (n1)<-[:extracted_from]-(n2)".format(subject_info['id'],sample_info['id']))
    cypher.append("MATCH (n2:sample{{id:'{0}'}}),(n3:file{{id:'{1}'}}) MERGE (n2)<-[d:derived_from{{{2}}}]-(n3)".format(sample_info['id'],file_info['id'],prep_info['prop_str']))

    # flatten lists of lists, uniquifying as we go
    _add_unique_tags(all_tags, file_info['tag_list'])
    _add_unique_tags(all_tags, prep_info['tag_list'])
    _add_unique_tags(all_tags, sample_info['tag_list'])
    _add_unique_tags(all_tags, visit_info['tag_list'])
    _add_unique_tags(all_tags, subject_info['tag_list'])
    _add_unique_tags(all_tags, study_info['tag_list'])
    _add_unique_tags(all_tags, project_info['tag_list'])

    unique_tags = []
    for k in all_tags:
        unique_tags.append(k)

        for tag in unique_tags:
            if ":" in tag:
                tag = tag.split(':',1)[1] # don't trim URLs and the like (e.g. http:)
                tag = tag.strip()
            if tag: # if there's something there, attach
                if tag.isspace():
                    continue
                cypher.append('MERGE (n:tag{{term:"{0}"}})'.format(tag))
                cypher.append('MATCH (n1:file{{id:"{0}"}}),(n2:tag{{term:"{1}"}}) MERGE (n2)<-[:has_tag]-(n1)'.format(file_info['id'],tag))

    return cypher

# Function to insert into Neo4j. Takes in Neo4j connection and a document.
def _insert_into_neo4j(doc):
    
    if doc is not None:

        if type(doc['prep']) is not list: # most common node with 1:1 file to prep
            return _generate_cypher(doc,'')

        else: # node with multiple upstream preps per file
            cypher_list = []

            for x in range(0,len(doc['prep'])):
                cypher_list += _generate_cypher(doc,x)

            return cypher_list

# Takes a dictionary from the OSDF doc and builds a list of the keys that are
# irrelevant.
def _delete_keys_from_dict(doc_dict):
    delete_us = []

    for key,val in doc_dict.items():
        if not val or not key:
            delete_us.append(key)

        # Unfortunately... have to check for keys comprised of blanks paces
        if len(key.replace(' ','')) == 0:
            delete_us.append(key)

    for empty in delete_us:
        del doc_dict[empty]

    return doc_dict


if __name__ == '__main__':

    # Set up an ArgumentParser to read the command-line
    parser = argparse.ArgumentParser(
        description="Dump documents out of CouchDB to the filesystem")

    parser.add_argument(
        '--db', type=str,
        help="The CouchDB database URL from which to load data")

    parser.add_argument(
        "--page_size", type=int, default=1000,
        help="How many documents to request from CouchDB in each batch.")

    parser.add_argument(
        "--neo4j_password", type=str, default="neo4j",
        help="The password for Neo4j")

    parser.add_argument(
        "--http_port", type=int, default=7474,
        help="The port for the exposed HTTP location")

    parser.add_argument(
        "--bolt_port", type=int, default=7687,
        help="The port for the exposed bolt location")

    parser.add_argument(
        "--batch_size", type=int, default=500,
        help="The batch size for Cypher statements to be committed")

    args = parser.parse_args()

    cy = Graph(password = args.neo4j_password, bolt_port = args.bolt_port, http_port = args.http_port)

    _build_constraint_index('subject','id',cy)
    _build_constraint_index('sample','id',cy)
    _build_constraint_index('file','id',cy)
    _build_constraint_index('token','id',cy)
    _build_constraint_index('tag','term',cy)
    _build_constraint_index('user','username',cy)
    _build_constraint_index('session','id',cy)
    _build_constraint_index('query','url',cy)

    # Now just loop through and create documents. I like counters, so there's
    # one to tell me how much has been done. I also like timers, so there's one
    # of them too.
    counter = 1
    start_time = time.time()

    # Dictionaries for each nodes where it goes like {project{id{couch_db_doc}}} so that
    # it is fast to look up IDs when traversing upstream.
    nodes = {
        'project': {},
        'study': {},
        'subject': {},
        'subject_attribute': {},
        'visit': {},
        'visit_attribute': {},
        'sample': {},
        'sample_attribute': {},
        'wgs_dna_prep': {},
        'host_seq_prep': {},
        'wgs_raw_seq_set': {},
        'wgs_raw_seq_set_private': {},
        'host_wgs_raw_seq_set': {},
        'microb_transcriptomics_raw_seq_set': {},
        'host_transcriptomics_raw_seq_set': {},
        'wgs_assembled_seq_set': {},
        'viral_seq_set': {},
        'annotation': {},
        'clustered_seq_set': {},
        '16s_dna_prep': {},
        '16s_raw_seq_set': {},
        '16s_trimmed_seq_set': {},
        'microb_assay_prep': {},
        'host_assay_prep': {},
        'proteome': {},
        'metabolome': {},
        'lipidome': {},
        'cytokine': {},
        'abundance_matrix': {}
    }

    files_only = {
        'wgs_raw_seq_set',
        'wgs_raw_seq_set_private',
        'host_wgs_raw_seq_set',
        'microb_transcriptomics_raw_seq_set',
        'host_transcriptomics_raw_seq_set',
        'wgs_assembled_seq_set',
        'viral_seq_set',
        'annotation',
        'clustered_seq_set',
        '16s_dna_prep',
        '16s_raw_seq_set',
        '16s_trimmed_seq_set',
        'microb_assay_prep',
        'host_assay_prep',
        'proteome',
        'metabolome',
        'lipidome',
        'cytokine',
        'abundance_matrix'
    }

    for doc in _all_docs_by_page(args.db, args.page_size):
        # Assume we don't want design documents, since they're likely to be
        # already stored elsewhere (e.g. in version control)
        if doc['id'].startswith("_design"):
            continue
        elif doc['id'].endswith("_hist"):
            continue

        # Clean up the document a bit. We don't need everything stored in
        # CouchDB for this instance.
        if 'value' in doc:
            del doc['value']
        if 'key' in doc:
            del doc['key']
        if '_id' in doc['doc']:
            del doc['doc']['_id']
        if '_rev' in doc['doc']:
            del doc['doc']['_rev']
        if 'acl' in doc['doc']:
            del doc['doc']['acl']
        if 'ns' in doc['doc']:
            del doc['doc']['ns']
        if 'subset_of' in doc['doc']['linkage']:
            del doc['doc']['linkage']['subset_of']

        # Clean up all these empty values
        doc['doc'] = _delete_keys_from_dict(doc['doc'])
        if 'meta' in doc['doc']:

            # Private nodes should have some mock URL data in them
            if 'urls' in doc['doc']['meta']:
                if len(doc['doc']['meta']['urls'])==1 and doc['doc']['meta']['urls'][0]== "":
                    doc['doc']['meta']['urls'][0] = 'Private:Private Data ({0})'.format(doc['id'])

            doc['doc']['meta'] = _delete_keys_from_dict(doc['doc']['meta'])

            if 'mixs' in doc['doc']['meta']:
                doc['doc']['meta']['mixs'] = _delete_keys_from_dict(doc['doc']['meta']['mixs'])

            if 'mimarks' in doc['doc']['meta']:
                doc['doc']['meta']['mimarks'] = _delete_keys_from_dict(doc['doc']['meta']['mimarks'])

        # At this point we should have purged the document of all properties
        # that have no value attached to them.

        # Now move meta values a step outward and make them a base property instead of nested
        if 'meta' in doc['doc']:

            for key,val in doc['doc']['meta'].items():

                if isinstance(val,dict): # if a nested dict, extract

                    for ke,va in doc['doc']['meta'][key].items():

                        if isinstance(va,dict):
                            
                            for k,v in doc['doc']['meta'][key][ke].items(): 
                                if k and v:
                                    doc['doc'][k] = v

                        else:
                            if ke and va:
                                doc['doc'][ke] = va

                else:
                    if key and val:
                        doc['doc'][key] = val

            del doc['doc']['meta']

        doc['doc']['id'] = doc['id']

        # Fix the old syntax to make sure it reads 'attribute' and not just 'attr'
        if doc['doc']['node_type'].endswith("_attr"):
            doc['doc']['node_type'] = "{0}ibute".format(doc['doc']['node_type'])

        # Build a giant list of each node type
        if doc['doc']['node_type'] in nodes:

            # Also, for these nodes, assign their ID to be the same as the 
            # sample/visit/subject they associate with for easy lookups. 
            # The data will also be subset to the 'meta' section as that is
            # where the interesting information lies in the attribute nodes. 
            if doc['doc']['node_type'].endswith("attribute"):
                nodes[doc['doc']['node_type']][doc['doc']['linkage']['associated_with'][0]] = doc
            else:
                nodes[doc['doc']['node_type']][doc['id']] = doc

        else:
            print("Warning, skipping node with type: {0}".format(doc['doc']['node_type']))

        # no-op ?
        key = counter

        counter += 1
        if (counter % 1000) == 0:
            sys.stderr.write(str(counter) + '\r')
            sys.stderr.flush()

    # These erroneous test docs ought to be corrected at the OSDF level
    ignore_us = ['88af6472fb03642dd5eaf8cddc37b0f3','88af6472fb03642dd5eaf8cddc2f50b1',
        '88af6472fb03642dd5eaf8cddc2f07c1','88af6472fb03642dd5eaf8cddc712ed7',
        '932d8fbc70ae8f856028b3f67cfab1ed','b9af32d3ab623bcfbdce2ea3a502c015',
        '610a4911a5ca67de12cdc1e4b4014cd0','610a4911a5ca67de12cdc1e4b40135fe',
        '610a4911a5ca67de12cdc1e4b4014133','610a4911a5ca67de12cdc1e4b40156e8',
        '610a4911a5ca67de12cdc1e4b40164de','610a4911a5ca67de12cdc1e4b4017467',
        '610a4911a5ca67de12cdc1e4b4017ab9','9bb18fe313e7fe94bf243da07e000de0',
        '9bb18fe313e7fe94bf243da07e00107e','b9af32d3ab623bcfbdce2ea3a5016b61',
        '9bb18fe313e7fe94bf243da07e003ac0','419d64483ec86c1fb9a94025f3b94551',
        '88af6472fb03642dd5eaf8cddc70c8ec','88af6472fb03642dd5eaf8cddc70d1de',
        '858ed4564f11795ec13dda4c109b345f','67ff3a7b9227c8c6f1db4bbf2226fc4b',
        '67ff3a7b9227c8c6f1db4bbf2227079e','88af6472fb03642dd5eaf8cddc2f4cb4',
        '88af6472fb03642dd5eaf8cddc2f4340','194149ed5273e3f94fc60a9ba5001573',
        '194149ed5273e3f94fc60a9ba59d2c9f','88af6472fb03642dd5eaf8cddc2f5abe',
        '9bb18fe313e7fe94bf243da07e0032e4','88af6472fb03642dd5eaf8cddc2f3405',
        '194149ed5273e3f94fc60a9ba50069b0','88af6472fb03642dd5eaf8cddc714325',
        '5a950f27980b5d93e4c16da1243b7c05','5a950f27980b5d93e4c16da1243b821c']
    ignore = set(ignore_us)

    # build a list of all Cypher statements to build the entire DB
    cypher_statements = [] 

    for key in nodes:

        if key in files_only:

            if key == "16s_raw_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_16s_raw_seq_set_doc(nodes,nodes[key][id]))

            elif key == "16s_trimmed_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_16s_trimmed_seq_set_doc(nodes,nodes[key][id]))

            elif key.endswith("ome") or key == "cytokine":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_omes_doc(nodes,nodes[key][id]))

            elif key == "abundance_matrix":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_abundance_matrix_doc(nodes,nodes[key][id]))

            elif (
                key == "wgs_raw_seq_set" or key == "wgs_raw_seq_set_private" 
                or key == "host_wgs_raw_seq_set" or key == "host_transcriptomics_raw_seq_set"
                or key == "microb_transcriptomics_raw_seq_set"
                ):
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_wgs_transcriptomics_doc(nodes,nodes[key][id]))

            elif key == "wgs_assembled_seq_set" or key == "viral_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_wgs_assembled_or_viral_seq_set_doc(nodes,nodes[key][id]))

            elif key == "annotation":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_annotation_doc(nodes,nodes[key][id]))

            elif key == "clustered_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _insert_into_neo4j(_build_clustered_seq_set_doc(nodes,nodes[key][id]))

    # Build a list of unique elements so that the number of calls to Neo4j are 
    # reduced. This should help greatly with how many tags are present per node 
    # especially. Can't use set because we must maintain order. 
    unique_cypher = set()
    final_statements = []
    for cypher in cypher_statements:
        if cypher not in unique_cypher and cypher != '':
            final_statements.append(cypher)
            unique_cypher.add(cypher)

    cypher_statements = final_statements

    # Send Cypher in transactions with a number of statements sent at a time 
    # equal to args.batch_size
    for j in range(0,len(cypher_statements),args.batch_size):
        
        start = j
        stop = j+args.batch_size
        
        if stop > len(cypher_statements):
            stop = len(cypher_statements)

        tx = cy.begin()

        # know everything has to pass through here, so take advantage and do 
        # blanket syntax corrections
        for pos in range(start,stop): 
            #statement = cypher_statements[pos]
            #for k,v in syntax_dict.items():
                #statement = statement.replace(k,v)
            tx.append(cypher_statements[pos])

        tx.commit()

    # Here set some better syntax for the portal and override the original OSDF values
    cy.run('MATCH (n:sample) SET n.study_full_name=n.study_name')
    for old,new in study_name_dict.items():
        cy.run('MATCH (n:sample) WHERE n.study_name="{0}" SET n.study_name="{1}"'.format(old,new))
    cy.run("MATCH (PSS:subject) WHERE PSS.project_name = 'iHMP' SET PSS.project_name = 'Integrative Human Microbiome Project'")

    # Now build indexes on each unique property found in this newest data set
    _build_all_indexes('subject',cy)
    _build_all_indexes('sample',cy)
    _build_all_indexes('file',cy)

    # A little final message
    sys.stderr.write("Done! {0} documents in {1} seconds!\n".format(
        counter, time.time() - start_time))
