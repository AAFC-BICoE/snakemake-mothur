''' Snakemake implementation of the mothur MiSeq SOP available at
    http://www.mothur.org/wiki/MiSeq_SOP#Getting_started

threads = min(threads, cores) with cores being the number of cores specified at the command line (option --cores). On a cluster node, Snakemake always uses as many cores as available on that node. Hence, the number of threads used by a rule never exceeds the number of physically available cores on the node.
'''

configfile: "config.json"
workdir: config["workdir"]
dataset = 'stability'

summary_files = [
    ('file1','file2'),
    (,),
]

'''
workdir can be changed by passing in a different workdir 
snakemake --config workdir="data/miseq/"
'''



'''Python helper functions'''
def get_start_end_from_summary(filename):
    with open(filename) as f:
        for line in f:
            percentile, start, end, *extra = line.split('\t')
            if percentile == 'Median:':
                return (start, end)

'''Snakemake rules'''
rule all:
    input:
        "{dataset}.trim.contigs.good.fasta".format(dataset=dataset)

rule make_contigs:
    version: "0.1"
    input:
        "{dataset}.files".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.fasta".format(dataset=dataset),
        "{dataset}.contigs.groups".format(dataset=dataset),
    threads: 1
    shell: 
        '''
        mothur "#make.contigs(file={input}, processors={threads})"
        '''
               
'''
Full output from make_contigs:
stability.trim.contigs.qual
stability.contigs.report
stability.scrap.contigs.fasta
stability.scrap.contigs.qual
stability.contigs.groups
'''

rule screen_seqs:
    version: "0.1"
    input:
        fa="{dataset}.trim.contigs.fasta".format(dataset=dataset),
        groups="{dataset}.contigs.groups".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.fasta".format(dataset=dataset),
        "{dataset}.contigs.good.groups".format(dataset=dataset),
    shell:
        '''
        mothur "#screen.seqs(fasta={input.fa}, group={input.groups}, maxambig={config[maxambig]}, maxlength={config[maxlength]})";
        '''

'''
EVERYTHING ABOVE SHOULD BE REPLACED WITH TRIMMOMATIC
'''


rule unique_seqs:
    version:"0.1"
    input:
        good="{dataset}.trim.contigs.good.fasta".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.names".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.fasta".format(dataset=dataset),
    shell:
        '''
        mothur "#unique.seqs(fasta={input.good})"
        '''

rule count_seqs:
    version:"0.1"
    input:
        names="{dataset}.trim.contigs.good.names".format(dataset=dataset),
        groups="{dataset}.contigs.good.groups".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.count_table".format(dataset=dataset),
    shell:
        '''
        mothur "#count.seqs(name={input.names}, group={input.groups})";
        '''

rule pcr_seqs:
    version:"0.1"
    input:
        fasta=config['reference']
    output:
        "silva.bacteria.pcr.fasta"
    shell:
        '''
        mothur "#pcr.seqs(fasta={input.fasta},start={config[start]}, end={config[end]}, keepdots={config[keepdots]})"
        '''

rule align_seqs:
    version:"0.1"
    input:
        fasta = "{dataset}.trim.contigs.good.unique.fasta".format(dataset=dataset),
        ref = "silva.bacteria.pcr.fasta"
    output:
        "{dataset}.trim.contigs.good.unique.align".format(dataset=dataset)
    shell:
        '''
        mothur "#align.seqs(fasta={input.fasta},reference={input.ref})";
        touch somefile.txt
        '''

rule screen_seqs:
    version:"0.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.align".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.count_table".format(dataset=dataset),
        summary="{dataset}.trim.contigs.good.unique.summary".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.summary".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.align".format(dataset=dataset),
    run:
        start,end = get_start_end_from_summary(input.summary)
        os.system('''
            mothur "#screen.seqs(fasta={input.fasta},count={input.count},summary={input.summary},start={start},end={end},maxhomop={config[maxhomop]}"
        ''').format(start=start, end=end)
        
'''
stability.trim.contigs.good.unique.good.summary
stability.trim.contigs.good.unique.good.align
stability.trim.contigs.good.unique.bad.accnos
stability.trim.contigs.good.good.count_table
'''

rule filter_seqs:
    version:"0.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.align".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.fasta".format(dataset=dataset)
    shell:
        '''
        mothur "#filter.seqs(fasta={input.fasta}, vertical={config[vertical]}, trump={config[trump]})"
        '''

rule unique_seqs2:
    version:"0.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.filter.fasta".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.good.count_table".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.count_table".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.fasta".format(dataset=dataset),
    shell:
        '''
        mothur "#unique.seqs(fasta={input.fasta}, count={input.count})"
        '''

rule pre_cluster:
    version:"0.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.filter.unique.fasta".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.unique.good.filter.count_table".format(dataset=dataset)
    output:
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        "{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.count_table".format(dataset=dataset)
    shell:
        '''
        mothur "#pre.cluster(fasta={input.fasta}, count={input.count}, diffs={config[diff]})"
        '''

rule chimera_uchime:
    version: "0.1"
    input:
        fasta="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.fasta".format(dataset=dataset),
        count="{dataset}.trim.contigs.good.unique.good.filter.unique.precluster.count_table".format(dataset=dataset)
    output:
    shell:
        '''
        mothur "#chimera.uchime(fasta={input.fasta}, count={input.count}, dereplicate={config[dereplicate]}"
        '''

# rule summary_seq3:
#     version: "0.1"


# rule classify_seqs

# rule remove_lineage