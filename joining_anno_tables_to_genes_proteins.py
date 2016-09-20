from pyspark.sql.functions import explode, col

anno_dict = {'snpeff_hg38': 'anno_snpeff_hg38.snpeff_hg38_snpeff_4_2_2016_07_0cb53e3_2016_06_14_0',
             'refseq_genes': 'anno_refseq_genes.refseq_genes_2016_07_0cb53e3_2016_06_14_0',
             'uniprot_refseq_mapping': 'anno_uniprot_refseq_mapping.uniprot_refseq_mapping_2016_07_62e25d8_2016_06_13_0'}


snpeff = sqlContext.sql('select * from {}'.format(anno_dict['snpeff_hg38']))
snpeff_anno = snpeff.select('chromosome','position','ref','alt', explode(snpeff.annotation_records).alias('element'))
snpeff_anno.registerTempTable('snpeff_anno')


# anno_genes_transcripts = sqlContext.sql('''
# select distinct gene_symbol, feature_id
# from snpeff_anno 
# where feature_type = "transcript"
# and hgvs_p is not null''')
# anno_genes_transcripts.registerTempTable('anno_genes_transcripts')

uniprot_refseq = sqlContext.sql('select * from {}'.format(anno_dict['uniprot_refseq_mapping']))
refseq_genes = sqlContext.sql('select * from {}'.format(anno_dict['refseq_genes']))
refseq_genes.filter(col('taxonomy_id')==9606).drop('taxonomy_id').registerTempTable('refseq_genes_human') 
uniprot_refseq \
.select('refseq_transcript_id','basic_uniprot_id', 'uniprot_name', explode('protein_names').alias('protein_names')) \
.registerTempTable('uniprot_refseq')

genes_to_proteins = sqlContext.sql('''
    select * from
    (select r.*, g.feature_type, g.feature_id
    from refseq_genes_human r,
    (select distinct element.gene_symbol, 
     element.feature_type, element.feature_id
     from snpeff_anno) g
    where r.entrez_gene_symbol = g.gene_symbol) gr
    left outer join uniprot_refseq u
    on u.refseq_transcript_id = gr.feature_id
    where gr.feature_type = "transcript"''')
