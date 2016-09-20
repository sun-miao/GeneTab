from pyspark.sql.functions import explode, col
snpeff = sqlContext.sql('select * from anno_snpeff_hg38.snpeff_hg38_snpeff_4_2_2016_07_0cb53e3_2016_06_14_0')
anno_records = snpeff.select('chromosome','position','ref','alt', explode(snpeff.annotation_records).alias('element'))
snpeff_anno = anno_records.select('chromosome','position','ref','alt',
                    'element.gene_symbol',
                    'element.annotation','element.annotation_impact',
                    'element.feature_type','element.feature_id','element.hgvs_p',
                    'element.cdna_pos','element.cdna_len',
                    'element.amino_acid_pos','element.amino_acid_len')
snpeff_anno.registerTempTable('snpeff_anno')

anno_genes_transcripts = sqlContext.sql('''
select distinct gene_symbol, feature_id
from snpeff_anno 
where feature_type = "transcript"
and hgvs_p is not null''')
anno_genes_transcripts.registerTempTable('anno_genes_transcripts')


uniprot_refseq = sqlContext.sql('select * from anno_uniprot_refseq_mapping.uniprot_refseq_mapping_2016_07_62e25d8_2016_06_13_0')

refseq_genes = sqlContext.sql('select * from anno_refseq_genes.refseq_genes_2016_07_0cb53e3_2016_06_14_0')
refseq_genes.filter(col('taxonomy_id')==9606)\
.select('entrez_gene_id','entrez_gene_symbol','synonyms',
        'ensembl_id','omim_id','hgnc_id','hprd_id','vega_id','imgt_id','mirbase_id',
        'chromosome','cytoband','gene_description',
        'symbol_from_nomenclature_authority',
        'full_name_from_nomenclature_authority',
        'other_designations') \
.registerTempTable('refseq_genes')

example_entries = sqlContext.sql('''
select r.*, u.basic_uniprot_id, u.refseq_transcript_id, u.uniprot_name, u.protein_names
from anno_genes_transcripts a, refseq_genes r, uniprot_refseq u
where a.gene_symbol = r.entrez_gene_symbol
and a.feature_id = u.refseq_transcript_id
''')
