''' Snakemake implementation of the mothur MiSeq SOP available at
    http://www.mothur.org/wiki/MiSeq_SOP#Getting_started

threads = min(threads, cores) with cores being the number of cores specified at the command line (option --cores). On a cluster node, Snakemake always uses as many cores as available on that node. Hence, the number of threads used by a rule never exceeds the number of physically available cores on the node.
'''
configfile: "config.json"
workdir: config["workdir"]

'''
workdir can be changed by passing in a different workdir 
snakemake --config workdir="data/miseq/"
'''

rule all:
    input:
        "somefile.txt"

rule make_contigs:
    version: "0.1"
    input:
        "stability.files",
    output:
        "stability.trim.contigs.fasta",
        "stability.contigs.groups"
    threads: 1
    shell: 
        "mothur \"#make.contigs(file={input}, processors={threads})\""
               
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
        fa="stability.trim.contigs.fasta",
        groups="stability.contigs.groups",
    output:
        "stability.trim.contigs.good.fasta",
        "stability.contigs.good.groups",
    shell:
        '''
        mothur \"#screen.seqs(fasta={input.fa}, group={input.groups}, maxambig={config[maxambig]}, maxlength={config[maxlength]})\";
        touch somefile.txt
        '''

rule unique_seqs:
    version:"0.1"
    input:
        good="stability.trim.contigs.good.fasta"
    output:
        "stability.trim.contigs.good.names",
        "stability.trim.contigs.good.unique.fasta",
        "somefile.txt",
    shell:
        '''
        mothur \"#unique.seqs(fasta={input.good})\"
        touch somefile.txt
        '''


# rule count_seqs:
#     version:"0.1"
#     input:
#         names="stability.trim.contigs.good.names",
#         groups="stability.contigs.good.groups"
#     output:
#         "stability.trim.contigs.good.count_table"
#     shell:
#         '''
#         mothur \"#count.seqs(name={input.names}, group={input.groups}))\"
#         '''

# rule summary_seqs:
#     input:
#         table="stability.trim.contigs.good.count_table"
#     shell:
#         '''
#         mothur \"#summary.seqs(count={input.table})\"     
#         '''
