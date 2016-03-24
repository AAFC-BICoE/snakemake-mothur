''' Snakemake implementation of the mothur MiSeq SOP available at
    http://www.mothur.org/wiki/MiSeq_SOP#Getting_started

    © Copyright Government of Canada 2009-2016
    Written by: Tom Sitter, for Agriculture and Agri-Food Canada
'''
import os
from collections import namedtuple
Summary = namedtuple('Summary', ['start', 'end'])

configfile: "config.json"
dataset = config["dataset"]

'''
workdir can be changed by passing in a different workdir 
snakemake --config workdir="data/miseq/"
'''
workdir: config["workdir"]

'''Python helper functions'''
def parse_summary(wildcards):
    filename = "{dataset}.summary.txt".format(dataset=dataset)
    if os.path.isfile(filename):
        with open(filename) as f:
            for line in f:
                if line.startswith("Median"):
                    _, start, end, *extra = line.split('\t')
                    return Summary(start=start, end=end)
    return Summary(start=0, end=0)

'''
Snakemake Rules
'''

# Import Preprocessor
include: 'preprocess.mothur.Snakefile'
# include: 'preprocess.trimmomatic.Snakefile'

rule all:
    input:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.cons.taxonomy".format(dataset=dataset)

rule pcr:
    version:"1.36.1"
    input:
        fasta = config["reference"] + ".fasta"
    output:
        config["reference"] + ".pcr.fasta"
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
        ref = config["reference"] + ".pcr.fasta"
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
    output:
        "{dataset}.trim.contigs.good.unique.good.align".format(dataset=dataset),
        "{dataset}.trim.contigs.good.good.count_table".format(dataset=dataset),
    run:
        summary = parse_summary(input.summary)
        cmd = "mothur \"#screen.seqs(fasta={},count={},summary={},start={},end={},maxhomop={})\"".format(
               input.fasta, input.count, input.summary, summary.start, summary.end, config["maxhomop"])
        os.system(cmd)
        

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
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.accnos".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.count_table".format(dataset=dataset),
    shell:
        '''
        mothur "#chimera.uchime(fasta={input.fasta}, count={input.count}, dereplicate={config[dereplicate]})"
        '''

rule remove:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        accnos = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.accnos".format(dataset=dataset),
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
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.count_table".format(dataset=dataset),
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
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.count_table".format(dataset=dataset),
        tax = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.taxonomy".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.taxonomy".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pick.fasta".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.pick.count_table".format(dataset=dataset)

    log:
        "logs/remove_lineage/{dataset}.log".format(dataset=dataset)
    shell:
        '''
        mothur "#remove.lineage(fasta={input.fasta}, count={input.count}, taxonomy={input.tax}, taxon={config[taxon]})"
        '''

rule remove_groups:
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pick.fasta".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.pick.count_table".format(dataset=dataset),
        taxonomy="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.taxonomy".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.pick.pick.count_table".format(dataset=dataset),
	"{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.taxonomy".format(dataset=dataset),
    shell:
        '''
        mothur "#remove.groups(count={input.count}, fasta={input.fasta}, taxonomy={input.taxonomy}, groups={config[group]});"
        '''

rule phylotype:
    input:
        taxonomy = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.taxonomy".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.tx.list".format(dataset=dataset)
    shell:
        '''
        mothur "#phylotype(taxonomy={input.taxonomy})"
        '''

rule classify_phylotypes:
    input:
        list = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.tx.list".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.pick.count_table".format(dataset=dataset),
        taxonomy = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.taxonomy".format(dataset=dataset),
    output:
        'idk'     
    shell:
        '''
        mothur "#make.shared(list={input.list}, count={input.count}, label=0.03);
               classify.otu(list={input.list}, count={input.count}, taxonomy={input.taxonomy}, label=0.03);"
        '''

# Analysis
rule analysis:
    input:
        taxonomy = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.taxonomy".format(dataset=dataset),
        count = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.uchime.pick.pick.pick.count_table".format(dataset=dataset),
        list = "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.tx.list".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.pick.pds.wang.pick.pick.cons.taxonomy".format(dataset=dataset),
    shell:
        '''
        mothur "#make.shared(list={input.list}, count={input.count}, label=1);
                classify.otu(list={input.list}, count={input.count}, taxonomy={input.taxonomy}, label=1);"
        '''
