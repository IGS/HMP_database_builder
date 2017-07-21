#!/usr/bin/python
#
# Contains accessories (Some functions and some dicts) to convert from OSDF syntax to what will be loaded in Neo4j.

# This function takes in a unicode value found in the couch dump and will replace quotes,
# either single or double, with literal quotes so that these can be passed to a Cypher 
# statement without escaping out early. May seem a bit round-about, but essentially 
# each unicode value needs to turn into a string in order to do replacement of quotes
# and must be sent back to unicode for Python processing into an eventual Cypher query.
def mod_quotes(val):
    if isinstance(val, unicode):
        val = val.encode('utf-8')
        val = val.replace("'",r"\'")
        val = val.replace('"',r'\"')
        val = val.decode('utf-8')
        # In order to search the DB as you would expect, convert number only strings to digits
        if val.isdigit():
            val = float(val) # float just in case
    return val

# This dictionary simply reformats aspects like capitalization for Neo4j. 
nodes = {
    'project': 'Case',
    'study': 'Case',
    'subject': 'Case',
    'subject_attr': 'Case',
    'subject_attribute': 'Case',
    'visit': 'Case',
    'visit_attr': 'Case',
    'visit_attribute': 'Case',
    'sample': 'Case',
    'sample_attr': 'Case',
    'sample_attribute': 'Case',
    'wgs_dna_prep': 'File',
    'host_seq_prep': 'File',
    'wgs_raw_seq_set': 'File',
    'wgs_raw_seq_set_private': 'File',
    'host_wgs_raw_seq_set': 'File',
    'microb_transcriptomics_raw_seq_set': 'File',
    'host_transcriptomics_raw_seq_set': 'File',
    'wgs_assembled_seq_set': 'File',
    'viral_seq_set': 'File',
    'annotation': 'File',
    'clustered_seq_set': 'File',
    '16s_dna_prep': 'File',
    '16s_raw_seq_set': 'File',
    '16s_trimmed_seq_set': 'File',
    'microb_assay_prep': 'File',
    'host_assay_prep': 'File',
    'proteome': 'File',
    'metabolome': 'File',
    'lipidome': 'File',
    'cytokine': 'File',
    'abundance_matrix': 'File',
    'tags': 'Tags',
    'mimarks': 'MIMARKS',
    'mixs': 'Mixs'
}

# These are all the different edge types present in the schema. 
# Note that 'subset_of' will be removed after loading in order to 
# comply with iHMP schema.
edges = {
    'part_of': 'PART_OF',
    'subset_of': 'SUBSET_OF',
    'participates_in': 'PARTICIPATES_IN',
    'associated_with': 'ASSOCIATED_WITH',
    'by': 'BY',
    'collected_during': 'COLLECTED_DURING',
    'prepared_from': 'PREPARED_FROM',
    'sequenced_from': 'SEQUENCED_FROM',
    'derived_from': 'DERIVED_FROM',
    'computed_from': 'COMPUTED_FROM',
    'has_tag': 'HAS_TAG',
    'has_mimarks': 'HAS_MIMARKS',
    'has_mixs': 'HAS_MIXS'
}

# Known edges used in the dump scripts
definitive_edges = {
    'part_of': 'Case',
    'subset_of': 'Case',
    'participates_in': 'Case',
    'by': 'Case',
    'associated_with': 'Case',
    'collected_during': 'Case',
    'prepared_from': 'Case',
    'has_tag': 'Tags',
    'has_mimarks': 'MIMARKS',
    'has_mixs': 'Mixs'
}

# Known edges used in the mirror script
definitive_edges2 = {
    'part_of': 'Case',
    'subset_of': 'Case',
    'participates_in': 'Case',
    'by': 'Case',
    'associated_with': 'Case',
    'collected_during': 'Case',
    'prepared_from': 'Case', 
    'computed_from': 'File' 
}

# Remap the study names using these values
study_name_dict = {
    'Human microbiome project 16S production phase I.':'16S-PP1',
    'Human microbiome project 16S production phase II.':'16S-PP2',
    'Skin Microbiome in Disease States: Atopic Dermatitis and Immunodeficiency.':'16S-SM-ADI',
    'The Thrifty Microbiome: The Role of the Gut Microbiota in Obesity in the Amish.':'16S-GM-AO',
    "Diet, Genetic Factors, and the Gut Microbiome in Crohn's Disease.":'16S-GM-CD2',
    'Foregut Microbiome in Development of Esophageal Adenocarcinoma.':'16S-GM-EA',
    'The Role of the Gut Microbiota in Ulcerative Colitis, Targeted Gene Survey.':'16S-GM-UC',
    'The Human Microbiome in Pediatric Abdominal Pain and Intestinal Inflammation.':'16S-GM-CGD',
    "Effect of Crohn's Disease Risk Alleles on Enteric Microbiota.":'16S-GM-CD',
    'Evaluation of the Cutaneous Microbiome in Psoriasis.':'16S-SM-P',
    'The Neonatal Microbiome and Necrotizing Enterocolitis.':'16S-GM-NE',
    'The Microbial Ecology of Bacterial Vaginosis: A Fine Scale Resolution Metagenomic Study.':'16S-VM-BV',
    'The Vaginal Microbiome: Disease, Genetics and the Environment, 16S Gene Survey.':'16S-VM-DGE',
    'Urethral Microbiome of Adolescent Males.':'16S-UM-AD',
    "Metagenomic Analysis of the Structure and Function of the Human Gut Microbiota in Crohn's Disease.":'WGS-GM-CD',
    'The Role of the Gut Microbiota in Ulcerative Colitis, Whole Metagenome Sequencing Project.':'WGS-GM-UC',
    'The Human Virome in Children And Its Relationship to Febrile Illness.':'WGS-VIR-FE',
    'Human microbiome project WGS production phase II.':'WGS-PP2',
    'Human microbiome project WGS production phase I.':'WGS-PP1',
    'ibdmdb':'IBDMDB',
    'momspi':'MOMS-PI',
    'Inflammatory Bowel Disease Multi-omics Database (IBDMDB)':'IBDMDB',
    'prediabetes':'T2D'
}

# Some basic syntax corrections
#syntax_dict = {
#    '16s':'16S',
#    'wgs':'WGS'
#}

# Remap data format values 
file_format_dict = {
    'sff':'Standard Flowgram File',
    'peptide_fsa':'Peptide FASTA',
    'gff3':'GFF3',
    'nucleotide_fsa':'Nucleotide FASTA',
    'null':'null',
    'fastq':'FASTQ',
    'biom':'Biological Observation Matrix',
    'fasta':'FASTA',
    'csv':'CSV',
    'mzXML':'Mass Spectroscopy Proteomics'
}

# Remap body product values
body_product_dict = {
    'Feces [FMA:64183]':'feces',
    'Saliva [FMA:59862]':'saliva',
    'vaginal mucosa [UBERON:0004983]':'vaginal mucosa',
    'Stool':'feces',
    'stool':'feces',
    'saliva [UBERON:0001836]':'saliva',
    'cervical mucus [UBERON:0012248]':'cervical mucus',
    'feces [UBERON:0001988]':'feces',
    'blood':'blood',
    'Nasal':'nasal',
    'urinary_tract':'urinary tract',
    'nasal':'nasal'
}

# Need this to add consistency to the body sites for query purposes. 
body_site_dict = {
    'abdomen': 'abdomen [FMA:9577]',
    'antecubital_fossa': 'cubital fossa [FMA:39848]',
    'anterior_nares': 'external naris [FMA:59645]',
    'attached_keratinized_gingiva': 'gingiva [FMA:59762]',
    'back': 'back [FMA:14181]',
    'blood': 'blood cell [FMA:62844]',
    'buccal_mucosa': 'buccal mucosa [FMA:59785]',
    'Buccal mucosa [FMA:59785]': 'buccal mucosa [FMA:59785]',
    'cervix': 'cervix of uterus [FMA:17740]',
    'Dorsum of tongue [FMA:54651]': 'dorsum of tongue [FMA:54651]',
    'elbow': 'elbow [FMA:24901]',
    'External naris [FMA:59645]': 'external naris [FMA:59645]',
    'FMA:276108': 'right nasal cavity [FMA:276108]',
    'FMA:326482': 'urinary tract [FMA:326482]',
    'FMA:64183': 'feces [FMA:64183]',
    'FMA:86713': 'peripheral blood mononuclear cell [FMA:86713]',
    'FMA:7842': 'angle of seventh rib [FMA:7842]',
    'foot': 'foot [FMA:9664]',
    'forearm': 'forearm [FMA:9663]',
    'Gastrointestinal tract [FMA:71132]': 'gastrointestinal tract [FMA:71132]',
    'gingiva [FMA:59762]': 'gingiva [FMA:59762]',
    'Gingiva [FMA:59762]': 'gingiva [FMA:59762]',
    'gut': 'gastrointestinal tract [FMA:71132]',
    'hand': 'hand [FMA:9712]',
    'hard_palate': 'hard palate [FMA:55023]',
    'Hard palate [FMA:55023]': 'hard palate [FMA:55023]',
    'head': 'head [FMA:7154]',
    'ileal_pouch': 'ileum [FMA:7208]',
    'ileum': 'ileum [FMA:7208]',
    'knee': 'knee [FMA:24974]',
    'left_antecubital_fossa': 'left cubital fossa [FMA:39850]',
    'left_retroauricular_crease': 'skin of left auriculotemporal part of head [FMA:70332]',
    'leg': 'leg [FMA:24979]',
    'mid_vagina': 'vagina [FMA:19949]',
    'nare': 'external naris [FMA:59645]',
    'nasal': 'nasal cavity [FMA:54378]',
    'nasopharynx': 'nasopharynx [FMA:54878]',
    'Nasopharynx [FMA:54878]': 'nasopharynx [FMA:54878]',
    'oral_cavity': 'oral cavity [FMA:20292]',
    'Oral cavity [FMA:20292]': 'oral cavity [FMA:20292]',
    'Orifice of vagina [FMA:19984]': 'orifice of vagina [FMA:19984]',
    'Palantine tonsil [FMA:9610]': 'palatine tonsil [FMA:9610]',
    'Palatine tonsil [FMA:9610]': 'palatine tonsil [FMA:9610]',
    'palatine_tonsils': 'palatine tonsil [FMA:9610]',
    'perianal_region': 'perianal space [FMA:29719]',
    'Plasma [FMA:62970]': 'plasma [FMA:62970]',
    'popliteal_fossa': 'popliteal fossa [FMA:22525]',
    'posterior_fornix': 'posterior fornix of vagina [FMA:19987]',
    'Posterior fornix of vagina [FMA:19987]': 'posterior fornix of vagina [FMA:19987]',
    'rectal': 'rectum [FMA:14544]',
    'right_antecubital_fossa': 'right cubital fossa [FMA:39849]',
    'right cubital fossa [FMA:39849]': 'right cubital fossa [FMA:39849]',
    'right_retroauricular_crease': 'skin of right auriculotemporal part of head [FMA:70331]',
    'saliva': 'portion of saliva [FMA:59862]',
    'scalp': 'scalp [FMA:46494]',
    'shin': 'anterior part of leg [FMA:24985]',
    'shoulder': 'shoulder [FMA:25202]',
    'Skin of left auriculotemporal part of head [FMA:70332]': 'skin of left auriculotemporal part of head [FMA:70332]',
    'Skin of right auriculotemporal part of head [FMA:70331]': 'skin of right auriculotemporal part of head [FMA:70331]',
    'stool': 'feces [FMA:64183]',
    'subgingival_plaque': 'gingiva [FMA:59762]',
    'supragingival_plaque': 'gingiva [FMA:59762]',
    'test': 'test',
    'thigh': 'thigh [FMA:24967]',
    'throat': 'throat [FMA:228738]',
    'Throat [FMA:228738]': 'throat [FMA:228738]',
    'tongue_dorsum': 'dorsum of tongue [FMA:54651]',
    'unknown': 'unknown',
    'urethra': 'urethra [FMA:19667]',
    'urinary_tract': 'urinary tract [FMA:326482]',
    'Vagina [FMA:19949]': 'vagina [FMA:19949]',
    'vaginal': 'vagina [FMA:19949]',
    'vaginal_introitus': 'orifice of vagina [FMA:19984]',
    'volar_forearm': 'forearm [FMA:9663]',
    'wall_of_vagina': 'wall of vagina [FMA:19971]',
}

# A dict that purges the FMA code from the data
fma_free_body_site_dict = {
    'abdomen': 'abdomen',
    'antecubital_fossa': 'cubital fossa',
    'anterior_nares': 'external naris',
    'attached_keratinized_gingiva': 'gingiva',
    'back': 'back',
    'blood': 'blood cell',
    'buccal_mucosa': 'buccal mucosa',
    'Buccal mucosa [FMA:59785]': 'buccal mucosa',
    'cervix': 'cervix of uterus',
    'Dorsum of tongue [FMA:54651]': 'dorsum of tongue',
    'elbow': 'elbow',
    'External naris [FMA:59645]': 'external naris',
    'FMA:276108': 'right nasal cavity',
    'FMA:326482': 'urinary tract',
    'FMA:64183': 'feces',
    'FMA:86713': 'peripheral blood mononuclear cell',
    'FMA:7842': 'angle of seventh rib',
    'foot': 'foot',
    'forearm': 'forearm',
    'Gastrointestinal tract [FMA:71132]': 'gastrointestinal tract',
    'gingiva [FMA:59762]': 'gingiva',
    'Gingiva [FMA:59762]': 'gingiva',
    'gut': 'gastrointestinal tract',
    'hand': 'hand',
    'hard_palate': 'hard palate',
    'Hard palate [FMA:55023]': 'hard palate',
    'head': 'head',
    'ileal_pouch': 'ileum',
    'ileum': 'ileum',
    'knee': 'knee',
    'left_antecubital_fossa': 'left cubital fossa',
    'left_retroauricular_crease': 'skin of left auriculotemporal part of head',
    'leg': 'leg',
    'mid_vagina': 'vagina',
    'nare': 'external naris',
    'nasal': 'nasal cavity',
    'nasopharynx': 'nasopharynx',
    'Nasopharynx [FMA:54878]': 'nasopharynx',
    'oral_cavity': 'oral cavity',
    'Oral cavity [FMA:20292]': 'oral cavity',
    'Orifice of vagina [FMA:19984]': 'orifice of vagina',
    'Palantine tonsil [FMA:9610]': 'palatine tonsil',
    'Palatine tonsil [FMA:9610]': 'palatine tonsil',
    'palatine_tonsils': 'palatine tonsil',
    'perianal_region': 'perianal space',
    'Plasma [FMA:62970]': 'plasma',
    'popliteal_fossa': 'popliteal fossa',
    'posterior_fornix': 'posterior fornix of vagina',
    'Posterior fornix of vagina [FMA:19987]': 'posterior fornix of vagina',
    'rectal': 'rectum',
    'right_antecubital_fossa': 'right cubital fossa',
    'right cubital fossa [FMA:39849]': 'right cubital fossa',
    'right_retroauricular_crease': 'skin of right auriculotemporal part of head',
    'saliva': 'portion of saliva',
    'scalp': 'scalp',
    'shin': 'anterior part of leg',
    'shoulder': 'shoulder',
    'Skin of left auriculotemporal part of head [FMA:70332]': 'skin of left auriculotemporal part of head',
    'Skin of right auriculotemporal part of head [FMA:70331]': 'skin of right auriculotemporal part of head',
    'stool': 'feces',
    'subgingival_plaque': 'gingiva',
    'supragingival_plaque': 'gingiva',
    'test': 'test',
    'thigh': 'thigh',
    'throat': 'throat',
    'Throat [FMA:228738]': 'throat',
    'tongue_dorsum': 'dorsum of tongue',
    'unknown': 'unknown',
    'urethra': 'urethra',
    'urinary_tract': 'urinary tract',
    'Vagina [FMA:19949]': 'vagina',
    'vaginal': 'vagina',
    'vaginal_introitus': 'orifice of vagina',
    'volar_forearm': 'forearm',
    'wall_of_vagina': 'wall of vagina',
}