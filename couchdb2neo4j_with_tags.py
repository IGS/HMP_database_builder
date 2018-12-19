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

import argparse,gzip,json,os,requests,sys,time,urllib
from py2neo import Graph
from accs_for_couchdb2neo4j import fma_free_body_site_dict, study_name_dict, file_format_dict, node_type_mapping
from accs_for_couchdb2neo4j import file_nodes, meta_to_keep, meta_null_vals, keys_to_keep, ignore
import pprint
import re

# nodes without upstream SRS#
NO_UPSTREAM_SRS = {}
# unique tag terms
TAGS = {}
UNIQUE_LINKS = {}
# node ids of inserted nodes
NODE_IDS = {}
# nodes to insert, grouped by type (e.g. 'file', 'tag', etc.)
NODES = { 'file': [] , 'sample': [], 'subject': [], 'tag': [] }

# cypher queries to perform link insertion
SUBJ_SAMPLE_CYPHER = "UNWIND $objects as o MATCH (n1:subject{id: o.subject_id}),(n2:sample{id: o.sample_id}) MERGE (n1)<-[:extracted_from]-(n2)"
FILE_TAG_CYPHER = "UNWIND $objects as o MATCH (n1:file{id: o.file_id}),(n2:tag{term: o.term}) MERGE (n2)<-[:has_tag]-(n1)"
# "<PROPS>" indicates where the specific properties will be substituted in
SAMPLE_FILE_CYPHER = "UNWIND $objects as o MATCH (n2:sample{id: o.sample_id}),(n3:file{id: o.file_id}) MERGE (n2)<-[d:derived_from{ <PROPS> }]-(n3)"

# track node links/edges to insert
NODE_LINKS = { 
    'subject-sample': { 'cypher': SUBJ_SAMPLE_CYPHER, 'links': [] }, 
    'file-tag': { 'cypher': FILE_TAG_CYPHER, 'links': [] },
    'sample-file': { 'cypher': SAMPLE_FILE_CYPHER, 'links': [] },
    }

# track nodes added by node_type
NODES_BY_TYPE = {}

def _add_type(t):
    if t in NODES_BY_TYPE:
        NODES_BY_TYPE[t] += 1
    else:
        NODES_BY_TYPE[t] = 0

def _print_error(message):
    """
    Print a message to stderr, with a newline.
    """
    sys.stderr.write(str(message) + "\n")
    sys.stderr.flush()

def _all_docs_by_page(db_url, db_login, db_password, cache_dir=None, page_size=10):
    """
    Helper function to request documents from CouchDB in batches ("pages") for
    efficiency, but present them as a stream.
    """
    # Tell CouchDB we only want a page worth of documents at a time, and that
    # we want the document content as well as the metadata
    view_arguments = {'limit': page_size, 'include_docs': "true"}
    db_auth = None
    if db_login is not None:
        db_auth = ( db_login, db_password )

    # Keep track of the last key we've seen
    last_key = None

    # Option to create subdirectory to cache data retrieved from CouchDB.
    # This is intended primarily for debugging/testing purposes.
    cache_subdir = None
    if cache_dir is not None:
        # to keep things simple the cache will be page-size-specific
        cache_subdir = os.path.join(cache_dir, urllib.quote_plus(db_url), str(page_size))
        # create the subdir if it does not exist
        if not os.path.exists(cache_subdir):
            os.makedirs(cache_subdir)

    # retrieve a single page from either the on-disk cache or the CouchDB server
    def get_page(pagenum, params):
        page = None
        cache_page = None
        # check cache for hit
        if cache_subdir is not None:
            cache_page = os.path.join(cache_subdir, ("p%010d" % pagenum) + ".json.gz")
            if os.path.exists(cache_page):
                with gzip.open(cache_page, 'rb') as cfile:
                    page_content = cfile.read()
                    page = { "status_code": 200, "content": page_content, "source": "cache" }

        if page is None:
            response = requests.get(db_url + "/_all_docs", params=params, auth=db_auth)
            page = { "status_code": response.status_code, "content": response.content, "source": "DB" }
            # write page to cache
            if (cache_page is not None) and (response.status_code == 200):
                with gzip.open(cache_page, 'wb') as cfile:
                    cfile.write(response.content)
        return page

    pagenum = 1

    # retrieve all CouchDB documents
    while True:
        page = get_page(pagenum, params=view_arguments)
        pagenum += 1

        # If there's been an error, stop looping
        if page['status_code'] != 200:
            _print_error("Error from DB: " + str(page['content']))
            break

        # Parse the results as JSON. If there's an error, stop looping
        try:
            results = json.loads(page['content'])
        except:
            _print_error("Unable to parse JSON: " + str(page['content']))
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

    else:
        # create dummy prep for abundance matrix computed_from a study
        doc['prep'] = {}

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

def _build_alignment_or_host_variant_call_doc(all_nodes_dict,node):

    doc = {}
    which_upstream,which_prep = ("" for i in range(2))

    doc['main'] = node['doc']

    link = _refine_link(doc['main']['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_assembled_seq_set']:
        which_upstream = 'wgs_assembled_seq_set'
        doc[which_upstream] = _find_upstream_node(all_nodes_dict[which_upstream],which_upstream,link)
        link = _refine_link(doc[which_upstream]['linkage']['computed_from'])

    if link in all_nodes_dict['wgs_raw_seq_set']:
        which_upstream = 'wgs_raw_seq_set'
    elif link in all_nodes_dict['wgs_raw_seq_set_private']:
        which_upstream = 'wgs_raw_seq_set_private'
    elif link in all_nodes_dict['host_wgs_raw_seq_set']:
        which_upstream = 'host_wgs_raw_seq_set'
    else:
        _print_error("can't find upstream type for " + link)

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

    subtype = doc['main']['subtype']
    if subtype not in NO_UPSTREAM_SRS:
        NO_UPSTREAM_SRS[subtype] = {}
    NO_UPSTREAM_SRS[subtype][doc['main']['id']] = True

    # certain nodes are expected to map to multiple upstream SRS ids:
    #    wgs_assembled_seq_set/wgs_coassembly (for coassembled iHMP samples)
    #    16s_trimmed_seq_set/trimmed_16s (for multiplexed JCVI samples)
    #    wgs_assembled_seq_set/wgs_assembly for body-site-specific assemblies
    if ((doc['main']['subtype'] != 'wgs_coassembly' and doc['main']['subtype'] != 'trimmed_16s') 
        and
        (doc['main']['subtype'] != 'wgs_assembly' and doc['main']['name'] != "Body-site specific assemblies")):
        pp = pprint.PrettyPrinter(indent=4, stream=sys.stdout)
        sys.stdout.write("SRS# cannot be found upstream for node of type/subtype = {0}/{1}\n".format(doc['main']['node_type'], doc['main']['subtype']))
        pp.pprint(doc)

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

    # debug missing preps
    if 'prep' not in doc:
        pp = pprint.PrettyPrinter(indent=4, stream=sys.stdout)
        sys.stdout.write("prep not found for node of type/subtype = {0}/{1}\n".format(doc['main']['node_type'], doc['main']['subtype']))
        pp.pprint(doc)
        return None

    # some abundance matrices are computed_from the study rather than a specific sample/prep
    if doc['main']['node_type'] == 'abundance_matrix' and 'linkage' not in doc['prep']:
        doc['study'] = _find_upstream_node(all_nodes_dict['study'],'study',doc['main']['linkage']['computed_from'])
        doc['project'] = _find_upstream_node(all_nodes_dict['project'],'project',doc['study']['linkage']['part_of'])
    else:
        doc['sample'] = _find_upstream_node(all_nodes_dict['sample'],'sample',doc['prep']['linkage']['prepared_from'])
        doc['visit'] = _find_upstream_node(all_nodes_dict['visit'],'visit',doc['sample']['linkage']['collected_during'])
        doc['subject'] = _find_upstream_node(all_nodes_dict['subject'],'subject',doc['visit']['linkage']['by'])
        doc['study'] = _find_upstream_node(all_nodes_dict['study'],'study',doc['subject']['linkage']['participates_in'])
        doc['project'] = _find_upstream_node(all_nodes_dict['project'],'project',doc['study']['linkage']['part_of'])

    # Skip all the dummy data associated with the "Test Project"
    if doc['project']['id'] == '610a4911a5ca67de12cdc1e4b40018e1':
        return None
    elif 'prep' in doc:
        doc = _append_attribute_data(all_nodes_dict,doc)
    return doc

# This function appends attribute data to the current node doc type.
# Note this will only occur if there is such data available. Takes
# in all the nodes and the current doc to update
def _append_attribute_data(all_nodes_dict,doc):

    attributes = ['sample_attribute','visit_attribute','subject_attribute']

    for attr in attributes:
        if attr in all_nodes_dict: # only act if attr data is present

            node_to_add_to = attr.split('_')[0]
            if node_to_add_to not in doc:
                continue

            if type(doc[node_to_add_to]) is list:
                for x in range(0,len(doc[node_to_add_to])):
                    if doc[node_to_add_to][x]['id'] in all_nodes_dict[attr]:
                        for k,v in all_nodes_dict[attr][doc[node_to_add_to][x]['id']]['doc'].items():
                            if k in meta_to_keep: # only persist new/relevant information
                                v = _standardize_value(v)
                                if v is not None:
                                    doc[node_to_add_to][x][k] = v

            else:
                if doc[node_to_add_to]['id'] in all_nodes_dict[attr]:
                    for k,v in all_nodes_dict[attr][doc[node_to_add_to]['id']]['doc'].items():
                        if k in meta_to_keep:
                            v = _standardize_value(v)
                            if v is not None:
                                doc[node_to_add_to][k] = v

    return doc

# Function to test a value for type and return a consistent data type across
# various attr values. Already have purged null values, so simply pass along
# bools/ints and make sure any strings aren't in the null set defined by
# meta_null_vals.
def _standardize_value(value):

    if not isinstance(value,str):
        if isinstance(value,bool):
            return str(value)
        else:
            if str(value).lower() not in meta_null_vals:
                return value
    else:
        if value.lower() not in meta_null_vals:
            return value
        else:
            return None

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
    stime = time.time()
    cstr = "CREATE CONSTRAINT ON (x:{0}) ASSERT x.{1} IS UNIQUE".format(node,prop)
    cy.run(cstr)
    etime = time.time()
    _print_error("built unique constraint index on {0}.{1} in {2:.2f} second(s)".format(node, prop, etime-stime))

# Build indexes for searching on all the props that aren't ID. Takes which node
# to build all indexes on as well as the Neo4j connection.
def _build_all_indexes(node,cy):
    stime = time.time()
    result = cy.run("MATCH (n:{0}) WITH DISTINCT keys(n) AS keys UNWIND keys AS keyslisting WITH DISTINCT keyslisting AS allfields RETURN allfields".format(node))
    n_indexes = 0
    for x in result:
        prop = x['allfields']
        if prop != 'id':
            cy.run("CREATE INDEX ON :{0}(`{1}`)".format(node,prop))
            n_indexes += 1
    etime = time.time()
    _print_error("built {0} {1} indexes in {2:.2f} second(s)".format(n_indexes, node, etime-stime))

# Apply body site rewrites from fma_free_body_site_dict
def _mod_body_site(val):

    if isinstance(val, list):
        for x in val:
            if x in fma_free_body_site_dict:
                x = fma_free_body_site_dict[x]

    else:
        if val in fma_free_body_site_dict:
            val = fma_free_body_site_dict[val]

    return val

# Add dependent File node attributes based on node_type_mapping
def _add_dependent_file_attributes(doc,file_info):

    # convert file_info props to dict
    fip = {}
    for p in file_info['props']:
        fip[p['key']] = p['value']

    node_type = doc['main']['node_type']
    node_type2 = fip['node_type']
    if node_type != node_type2:
        print("node type mismatch")
        sys.exit(1)

    if node_type not in node_type_mapping:
        _print_error("no mapping defined for node type " + node_type)
        # debug missing node_type
        pp = pprint.PrettyPrinter(indent=4, stream=sys.stdout)
        print("doc=")
        pp.pprint(doc)
        print("file_info=")
        pp.pprint(file_info)
        return

    res = node_type_mapping[node_type]
    _add_type(node_type)

    # secondary mapping based on matrix type
    if '_key' in res:
        matrix_type = doc['main']['matrix_type']
        matrix_type2 = fip['matrix_type']
        if matrix_type != matrix_type2:
            print("matrix type mismatch")
            sys.exit(1)
        
        if matrix_type not in res:
            _print_error("no mapping defined for matrix type " + matrix_type)
            return
            
        _add_type(node_type + "/" + matrix_type)
        res = res[matrix_type]
            
    for key in res:
        if isinstance(res[key], dict):
            if res[key]['_key'] == 'parent':
                # check parent assay to determine whether organism_type should be 'host' or 'bacterial'
                prep_type = doc['prep']['node_type']
                if prep_type == 'host_assay_prep':
                    file_info['props'].append({'key': key, 'value': 'host'})
                else:
                    _print_error("unrecognized prep type encountered: " + prep_type)
                    sys.exit(1)
            else:
                _print_error("unknown _key value of " + res[key]['_key'] + " in node_type_mapping")
        else:
            file_info['props'].append({'key': key, 'value': res[key]})

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

    if focal_node not in doc:
        return {'id':doc_id,'tag_list':tags,'prop_str':"",'props':props}

    if index == '':
        relevant_doc = doc[focal_node]
    else:
        relevant_doc = doc[focal_node][index]

    for key,val in relevant_doc.items():
        if key == 'linkage' or not val: # document itself contains all linkage info already
            continue

        if isinstance(val, int) or isinstance(val, float):
            key = key.encode('utf-8')
            props.append({'key': '{0}{1}'.format(key_prefix,key), 'value': val })
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
                        props.append({'key': '{0}contact'.format(key_prefix), 'value': '{0}'.format(email) })
                        break
                    else:
                        props.append({'key': '{0}contact'.format(key_prefix), 'value': '{0}'.format(val[j]) })
                        break

                else:
                    endpoint = val[j].split(':')[0]
                    props.append({'key': '{0}{1}'.format(key_prefix,endpoint), 'value': '{0}'.format(val[j]) })
        else:
            val = _mod_body_site(val)
            key = key.encode('utf-8')
            val = val.encode('utf-8')
            props.append({'key': '{0}{1}'.format(key_prefix,key), 'value': '{0}'.format(val) })

        if key == "id":
            doc_id = val

    if focal_node == 'main': # missing file formats will default to text files (only true so far for lipidome)
        format_present = False
        for p in props:
            if p['key'] == 'format':
                format_present = True
                break

        if not format_present:
            props.append({'key': 'format', 'value': 'Text' })

    # remove empty properties and apply rewrites
    new_props = []
    new_prop_strs = []
    for prop in props:
        if (prop['key'] is None or prop['key'] == ""):
            continue

        # change syntax for file format and node_type
        if focal_node == 'main' and prop['key'] == 'format':
            for v1,v2 in file_format_dict.items():
                if prop['value'] == v1:
                    prop['value'] = v2
                    break

        new_props.append(prop)
        new_prop_strs.append('`{0}`:{1}'.format(prop['key'], prop['value']))

    props = new_props
    props_str = (',').join(new_prop_strs)

    return {'id':doc_id,'tag_list':tags,'prop_str':props_str,'props':props}

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
    _add_dependent_file_attributes(doc, file_info)

    if file_info['id'] not in NODE_IDS:
        NODE_IDS[file_info['id']] = True
        NODES['file'].append({'_props': file_info['props']})

    sample_info = _traverse_document(doc,'sample',index)
    visit_info = _traverse_document(doc,'visit',index)
    study_info = _traverse_document(doc,'study',index)
    sample_props = sample_info['props']
    sample_props.extend(visit_info['props'])
    sample_props.extend(study_info['props'])
    if sample_info['id'] not in NODE_IDS:
        NODE_IDS[sample_info['id']] = True
        NODES['sample'].append({'_props': sample_props})

    subject_info = _traverse_document(doc,'subject',index)
    project_info = _traverse_document(doc,'project',index)
    subject_props = subject_info['props']
    subject_props.extend(project_info['props'])
    props = "{0},{1}".format(subject_info['prop_str'],project_info['prop_str'])
    if subject_info['id'] not in NODE_IDS:
        NODE_IDS[subject_info['id']] = True
        NODES['subject'].append({'_props': subject_props})

    prep_info = _traverse_document(doc,'prep',index)

    # subject(id) <-[:extracted_from]-(n2) sample(id)
    lkey = ":".join([subject_info['id'], sample_info['id']])
    if lkey not in UNIQUE_LINKS:
        subj_sample_link = { 'subject_id': subject_info['id'], 'sample_id': sample_info['id'] }
        NODE_LINKS['subject-sample']['links'].append(subj_sample_link)
        UNIQUE_LINKS[lkey] = True

    # add sample - file link
    # sample(id) <-[:derived_from]-(n3) file(id) -> derived_from has associated properties
    lkey = ":".join([sample_info['id'], file_info['id']])

    # checking uniqueness of sample-file links is expensive because the link properties (minus 'id') must be examined:
    if args.check_sample_file_uniqueness:
        prop_list = sorted(x for x in prep_info['props'] if x['key'] != 'id')
        pp = pprint.PrettyPrinter(indent=2)
        props_str = pp.pformat(prop_list)

        if lkey in UNIQUE_LINKS:
            # dict of sorted PrettyPrinted properties seen thus far
            props_d = UNIQUE_LINKS[lkey]

            # link is an exact duplicate
            if props_str in props_d:
                _print_error("INFO - duplicate link with lkey=" + lkey + " and identical properties")
            else:
                props_d[props_str] = True
        else:
            UNIQUE_LINKS[lkey] = { props_str: True }

    sample_file_link = { 'sample_id': sample_info['id'], 'file_id': file_info['id'], '_props': prep_info['props'] }
    NODE_LINKS['sample-file']['links'].append(sample_file_link)

    # flatten lists of lists, uniquifying as we go
    _add_unique_tags(all_tags, file_info['tag_list'])
    _add_unique_tags(all_tags, prep_info['tag_list'])
    _add_unique_tags(all_tags, sample_info['tag_list'])
    _add_unique_tags(all_tags, visit_info['tag_list'])
    _add_unique_tags(all_tags, subject_info['tag_list'])
    _add_unique_tags(all_tags, study_info['tag_list'])
    _add_unique_tags(all_tags, project_info['tag_list'])

    for tag in all_tags:
        if ":" in tag:
            tag = tag.split(':',1)[1] # don't trim URLs and the like (e.g. http:)
            tag = tag.strip()
        if tag: # if there's something there, attach
            if tag.isspace():
                continue

            # file(id) <-[:has_tag]- tag(term)
            # add tag link if it hasn't already been added
            tlkey = ":".join([file_info['id'], tag])
            if tlkey not in UNIQUE_LINKS:
                tag_link = { 'file_id': file_info['id'], 'term': tag }
                NODE_LINKS['file-tag']['links'].append(tag_link)
                UNIQUE_LINKS[tlkey] = True

            # add tag if it hasn't already been seen
            if tag not in TAGS:
                TAGS[tag] = tag_link
                NODES['tag'].append({'_props': [{'key': 'term', 'value': tag }]})

    return cypher

# Function to insert into Neo4j. Takes in Neo4j connection and a document.
def _generate_cypher_statements(doc):

    if doc is not None:

        if type(doc['prep']) is not list: # most common node with 1:1 file to prep
            return _generate_cypher(doc,'')

        else: # node with multiple upstream preps per file
            cypher_list = []

            for x in range(0,len(doc['prep'])):
                cypher_list += _generate_cypher(doc,x)

            return cypher_list
    else:
        return ""

# Takes a dictionary from the OSDF doc and builds a list of the keys that are
# irrelevant.
def _delete_keys_from_dict(doc_dict):
    delete_us = []

    for key,val in doc_dict.items():
        if not val or not key:
            delete_us.append(key)

        # Unfortunately... have to check for keys comprised of blank spaces
        if len(key.replace(' ','')) == 0:
            delete_us.append(key)

    for empty in delete_us:
        del doc_dict[empty]

    return doc_dict

# Insert an element (n) into a dict (d) of lists indexed by key (k)
def _add_to_group(d, n, k):
    if k in d:
        d[k].append(n)
    else:
        d[k] = [n]
    
# Concatenate the properties of a node/edge to determine its signature.
def _get_properties_sig(node):
    return "||".join(sorted([p['key'] for p in node]))

# Generic Cypher insert function that makes use of UNWIND to perform fast
# batch inserts (with batch size set by args.batch_size.) 
#
# cy - Cypher Graph
# insert_cypher - Cypher UNWIND query to insert data. It may contain the string "<PROPS>".
# obj_list - List of objects (nodes or links/edges) to insert. This is a list of dicts 
#   that defines the attributes/fields referenced in insert_cypher. If insert_cypher 
#   contains the string "<PROPS>" then each dict must have a '_props' field mapping to
#   a list of { 'key': property_key, 'value': property_value }.
# obj_type - Type of object ('node' or 'link') to be inserted. Used only to print a 
#   status message.
#
# When the insert_cypher contains the string "<PARAMS>" the function will 
# substitute in the actual parameter list based on the '_params' defined by
# each object in obj_list. Note that the objects need not all define the 
# same '_params': the fuction automatically groups the objects so that those
# with the same parameter signature are inserted together (allowing the use 
# of a single Cypher query for each such group of objects.) This significantly
# improves the speed at which inserts can be processed.
#
def _do_cypher_insert(cy, insert_cypher, obj_list, obj_type):
    stime = time.time()
    sig_to_objs = {}

    # case 1: there are properties associated with the new nodes or links, 
    # indicated by the presence of "<PROPS>" in the cypher query
    if re.search(r'<PROPS>', insert_cypher):
        for obj in obj_list:
            props = obj['_props']
            sig = _get_properties_sig(props)
            _add_to_group(sig_to_objs, obj, sig)
    
    # case 2: there are no properties associated with the new nodes or links
    else:
        sig_to_objs[''] = obj_list

    # group updates by property signature
    for sig in sorted(sig_to_objs.keys()):
        o_list = sig_to_objs[sig]
        n_objs = len(o_list)

        # create cypher query by substituting in the actual property list
        props_cypher = ", ".join(["`" + p + "`: o.`" + p + "`" for p in sig.split("||")])
        ins_cypher = re.sub(r'<PROPS>', props_cypher, insert_cypher)

        # create list of dicts to pass to Neo4J driver
        # note that creating a new list is significantly faster than trying to reuse
        # the existing one due to the overhead of the existing _props list.
        new_o_list = []
        for obj in o_list:
            new_obj = {}
            for k in obj:
                if k == '_props':
                    for p in obj['_props']:
                        new_obj[p['key']] = p['value']
                else:
                    new_obj[k] = obj[k]
            new_o_list.append(new_obj)

        # do batched inserts with batch size = args.batch_size
        for start in range(0, n_objs, args.batch_size):
            b_stime = time.time()
            stop = start + args.batch_size
            if stop > n_objs:
                stop = n_objs
            tx = cy.begin()
            o_slice = new_o_list[start:stop]
            tx.run(ins_cypher, { 'objects': o_slice })
            tx.commit()
            b_etime = time.time()
# uncomment for batch-level timing info:
#            _print_error("commit() done, insert took {0:.02f} second(s)".format(b_etime - b_stime))

    etime = time.time()
    _print_error("inserted {0} {1} in {2:.2f} second(s)".format(len(obj_list), obj_type, etime-stime))

# Use generic Cypher insert function to insert new nodes with properties.
def _insert_nodes(cy, node_type):
    insert_cypher =  "UNWIND $objects as o MERGE (n:" + node_type + "{ <PROPS> })"
    node_list = NODES[node_type]
    _do_cypher_insert(cy, insert_cypher, node_list, node_type + " nodes")

# Use generic Cypher insert function to insert new links/edges, either with or without properties.
def _insert_links(cy, link_type):
    links = NODE_LINKS[link_type]
    l_cypher = links['cypher']
    l_list = links['links']
    _do_cypher_insert(cy, l_cypher, l_list, link_type + " links")
    
if __name__ == '__main__':

    # Set up an ArgumentParser to read the command-line
    parser = argparse.ArgumentParser(
        description="Convert OSDF documents from CouchDB to Neo4J in the format expected by the data portal")

    parser.add_argument(
        '--db', type=str,
        help="The CouchDB database URL from which to load data")

    parser.add_argument(
        '--couchdb_login', type=str,
        help="The CouchDB login/username.")

    parser.add_argument(
        '--couchdb_password', type=str,
        help="The CouchDB password.")

    parser.add_argument(
        '--cache_dir', type=str, required=False,
        help="Directory in which to cache/find pages downloaded from CouchDB (optional - used for testing).")

    parser.add_argument(
        "--page_size", type=int, default=1000,
        help="How many documents to request from CouchDB in each batch.")

    parser.add_argument(
        "--neo4j_host", type=str, default="localhost",
        help="The Neo4j server hostname")

    parser.add_argument(
        "--neo4j_password", type=str, default=None,
        help="The password for Neo4j")

    parser.add_argument(
        "--http_port", type=int, default=7474,
        help="The port for the exposed HTTP location")

    parser.add_argument(
        "--bolt_port", type=int, default=7687,
        help="The port for the exposed bolt location")

    parser.add_argument(
        "--batch_size", type=int, default=5000,
        help="The batch size for Cypher statements to be committed")

    parser.add_argument(
        "--check_sample_file_uniqueness", dest="check_sample_file_uniqueness", action="store_true",
        help="Check sample-file links for uniqueness. Slower because the properties must be checked.")

    args = parser.parse_args()
    cy = Graph(host = args.neo4j_host, password = args.neo4j_password, bolt_port = args.bolt_port, http_port = args.http_port) 

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
        'abundance_matrix': {},

        # new node types added 10/22/2018
        'reference_genome_project_catalog_entry' : {},
        'host_epigenetics_raw_seq_set' : {},
        'serology' : {},
        'metagenomic_project_catalog_entry' : {},
        'alignment' : {},
        'proteome_nonpride': {},
        'host_variant_call': {}
    }

    # count skipped nodes and print a summary at the end
    node_skip_counts = {}

    for doc in _all_docs_by_page(args.db, args.couchdb_login, args.couchdb_password, args.cache_dir, args.page_size):
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
                                    if ke in keys_to_keep:
                                        doc['doc']["{}_{}".format(ke,k)] = v
                                    else:
                                        doc['doc'][k] = v

                        else:
                            if ke and va:
                                doc['doc'][ke] = va

                else:
                    if key and val:
                        doc['doc'][key] = val

            del doc['doc']['meta']

        doc['doc']['id'] = doc['id'] # move everything into 'doc' key

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
                if len(doc['doc']['linkage']['associated_with']) > 0: # get around test uploads
                    nodes[doc['doc']['node_type']][doc['doc']['linkage']['associated_with'][0]] = doc
            else:
                nodes[doc['doc']['node_type']][doc['id']] = doc

        else:
            node_type = doc['doc']['node_type']
            if node_type in node_skip_counts:
                node_skip_counts[node_type] += 1
            else:
                node_skip_counts[node_type] = 1

        # no-op ?
        key = counter

        counter += 1
        if (counter % 1000) == 0:
            sys.stderr.write(str(counter) + '\r')
            sys.stderr.flush()

    # build a list of all Cypher statements to build the entire DB
    cypher_statements = []

    sys.stdout.write("skipped node counts:\n")
    for node_type in node_skip_counts:
        count = node_skip_counts[node_type]
        sys.stdout.write("  {0} : {1}\n".format(node_type, str(count)))
    sys.stdout.write("\n")

    for key in nodes:

        if key in file_nodes:
            
            if key == "16s_raw_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_16s_raw_seq_set_doc(nodes, nodes[key][id]))

            elif key == "16s_trimmed_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_16s_trimmed_seq_set_doc(nodes, nodes[key][id]))

            elif key.endswith("ome") or key == "cytokine" or key == "proteome_nonpride" or key == "serology":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_omes_doc(nodes, nodes[key][id]))

            elif key == "abundance_matrix":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_abundance_matrix_doc(nodes, nodes[key][id]))

            elif (
                key == "wgs_raw_seq_set" or key == "wgs_raw_seq_set_private"
                or key == "host_wgs_raw_seq_set" or key == "host_transcriptomics_raw_seq_set"
                or key == "microb_transcriptomics_raw_seq_set"
                or key == "host_epigenetics_raw_seq_set"
                ):
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_wgs_transcriptomics_doc(nodes, nodes[key][id]))

            elif key == "wgs_assembled_seq_set" or key == "viral_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_wgs_assembled_or_viral_seq_set_doc(nodes, nodes[key][id]))

            elif key == "annotation":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_annotation_doc(nodes, nodes[key][id]))

            elif key == "clustered_seq_set":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_clustered_seq_set_doc(nodes, nodes[key][id]))

            elif key == "alignment" or key == "host_variant_call":
                for id in nodes[key]:
                    if id not in ignore:
                        cypher_statements += _generate_cypher_statements(_build_alignment_or_host_variant_call_doc(nodes, nodes[key][id]))
                
            elif not re.search(r'_prep$', key):
                _print_error("skipping {0} File nodes of type {1}".format(len(nodes[key]), key))

    # report nodes without upstream SRS ids
    sys.stdout.write("nodes without upstream SRS ids:\n")
    for subtype in NO_UPSTREAM_SRS:
        count = len(NO_UPSTREAM_SRS[subtype])
        sys.stdout.write("  {0} : {1}\n".format(subtype, str(count)))
    sys.stdout.write("\n")

    # node counts by type
    print("node counts by type/subtype:")
    for t in sorted(NODES_BY_TYPE):
        print(t + ": " + str(NODES_BY_TYPE[t]))
    sys.stdout.write("\n")

    # insert nodes (this order appears to yield the best performance):
    _insert_nodes(cy, 'subject')
    _insert_nodes(cy, 'sample')
    _insert_nodes(cy, 'file')
    _insert_nodes(cy, 'tag')

    neo4j_ver = ".".join([str(x) for x in cy.database.kernel_version])
    # 3.4.5-specific workaround
    if neo4j_ver == "3.4.5":
        # these theoretically superfluous index statements appear to be critical 
        # for fast loading in 3.4.5 but slow down loading in 3.4.10:
        _build_all_indexes('file',cy)
        _build_constraint_index('tag','term',cy)

    # insert tag links
    _insert_links(cy, 'file-tag')
    # insert subject-sample links
    _insert_links(cy, 'subject-sample')
    # insert sample-file links
    _insert_links(cy, 'sample-file')
        
    # Here set some better syntax for the portal and override the original OSDF values
    stime = time.time()
    cy.run('MATCH (n:sample) SET n.study_full_name=n.study_name')
    _print_error("updated study full names in {0:.2f} second(s)".format(time.time() - stime))

    stime = time.time()
    for old, new in study_name_dict.items():
        cy.run('MATCH (n:sample) WHERE n.study_name="{0}" SET n.study_name="{1}"'.format(old,new))
    _print_error("updated study names in {0:.2f} second(s)".format(time.time() - stime))

    stime = time.time()
    cy.run("MATCH (PSS:subject) WHERE PSS.project_name = 'iHMP' SET PSS.project_name = 'Integrative Human Microbiome Project'")
    _print_error("updated project name in {0:.2f} second(s)".format(time.time() - stime))

    # Now build indexes on each unique property found in this newest data set
    _build_all_indexes('subject',cy)
    _build_all_indexes('sample',cy)

    # A little final message
    _print_error("Done! converted {0} CouchDB documents in {1} seconds!\n".format(counter, time.time() - start_time))
