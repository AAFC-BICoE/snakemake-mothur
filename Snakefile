''' Snakemake implementation of the mothur MiSeq SOP available at
    http://www.mothur.org/wiki/MiSeq_SOP#Getting_started

    © Copyright Government of Canada 2009-2016
    Written by: Tom Sitter, for Agriculture and Agri-Food Canada
'''

configfile: "config.json"
dataset = 'stability'

'''
workdir can be changed by passing in a different workdir 
snakemake --config workdir="data/miseq/"
'''
workdir: config["workdir"]

'''Python helper functions'''
def get_start_end_from_summary(wildcards):
    filename = "{dataset}.summary.txt".format(dataset=dataset)
    with open(filename) as f:
        for line in f:
            if line.startswith("Median"):
                _, start, end, *extra = line.split('\t')
                return (start, end)

'''
Snakemake Rules
'''

rule all:
    input:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pick.fasta".format(dataset=dataset),


# Import Preprocessor
# include: 'preprocess.mothur.Snakefile'
include: 'preprocess.trimmomatic.Snakefile'


rule pcr:
    version:"1.36.1"
    input:
        fasta = config['reference']
    output:
        "silva.bacteria.pcr.fasta"
    threads: 8
    shell:
        '''
        mothur "#pcr.seqs(fasta={input.fasta},start={config[start]}, end={config[end]}, keepdots={config[keepdots]}, processors={threads})"
        '''

rule unique:
    version:"1.36.1"
    input:
        good = "{dataset}.trim.contigs.good.fasta".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.names".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.fasta".format(dataset=dataset),
    shell:
        '''
        mothur "#unique.seqs(fasta={input.good})"
        '''

rule count:
    version:"1.36.1"
    input:
        names = "{dataset}.trim.contigs.good.names".format(dataset=dataset),
        groups = "{dataset}.contigs.good.groups".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.count_table".format(dataset=dataset),
    shell:
        '''
        mothur "#count.seqs(name={input.names}, group={input.groups})"
        '''

rule align:
    version:"1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.fasta".format(dataset=dataset),
        ref = "silva.bacteria.pcr.fasta"
    output:
        "{dataset}.trim.contigs.good.unique.align".format(dataset=dataset)
    shell:
        '''
        mothur "#align.seqs(fasta={input.fasta},reference={input.ref})"
        '''

rule summary:
    version:"1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.align".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.count_table".format(dataset=dataset),
    output:
        summary = "{dataset}.trim.contigs.good.unique.summary".format(dataset=dataset),
        median = "{dataset}.summary.txt".format(dataset=dataset)
    shell:
        '''
        mothur "#summary.seqs(fasta={input.fasta}, count={input.count})" 1> {output.median}
        '''

rule screen2:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.align".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.count_table".format(dataset=dataset),
        summary = "{dataset}.trim.contigs.good.unique.summary".format(dataset=dataset),
    params:
        start_end = get_start_end_from_summary
    output:
        "{dataset}.trim.contigs.good.unique.good.align".format(dataset=dataset),
        "{dataset}.trim.contigs.good.good.count_table".format(dataset=dataset),
    shell:
        '''
        mothur "#screen.seqs(fasta={input.fasta},count={input.count},summary={input.summary},start={params.start_end[0]},end={params.start_end[1]},maxhomop={config[maxhomop]})"
        '''

rule filter:
    version: "1.36.1"
    input:
        "{dataset}.trim.contigs.good.unique.good.align".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.fasta".format(dataset=dataset)
    shell:
        '''
        mothur "#filter.seqs(fasta={input}, vertical=T, trump=.)"
        '''

rule unique2:
    version:"1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.fasta".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.good.count_table".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.count_table".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.fasta".format(dataset=dataset)
    shell:
        '''
        mothur "#unique.seqs(fasta={input.fasta}, count={input.count})"
        '''

rule pre_cluster:
    version:"1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.unique.fasta".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.unique.good.filter.count_table".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.count_table".format(dataset=dataset)
    log:
        "logs/precluster/{dataset}.log".format(dataset=dataset)
    shell:
        '''
        mothur "#pre.cluster(fasta={input.fasta}, count={input.count}, diffs={config[diff]})"
        '''

rule chimera_uchime:
    version: "1.36.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.count_table".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.denovo.uchime.accnos".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.denovo.uchime.pick.count_table".format(dataset=dataset),
    shell:
        '''
        mothur "#chimera.uchime(fasta={input.fasta}, count={input.count}, dereplicate={config[dereplicate]})"
        '''

rule remove:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        accnos = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.denovo.uchime.accnos".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.fasta".format(dataset=dataset)
    shell:
        '''
        mothur "#remove.seqs(fasta={input.fasta}, accnos={input.accnos})"
        '''

rule classify:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.fasta".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.denovo.uchime.pick.count_table".format(dataset=dataset),
        ref = config["trainset"],
        tax = config["taxonomy"]
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.taxonomy".format(dataset=dataset)
    shell:
        '''
        mothur "#classify.seqs(fasta={input.fasta}, count={input.count}, reference={input.ref}, taxonomy={input.tax}, cutoff={config[cutoff]})"
        '''

rule remove_lineage:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.fasta".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.denovo.uchime.pick.count_table".format(dataset=dataset),
        tax = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.taxonomy".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pick.fasta".format(dataset=dataset),
    log:
        "logs/remove_lineage/{dataset}.log".format(dataset=dataset)
    shell:
        '''
        mothur "#remove.lineage(fasta={input.fasta}, count={input.count}, taxonomy={input.tax}, taxon={config[taxon]})" > {log}
        '''