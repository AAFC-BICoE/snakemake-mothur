'''
Preprocess results using mothur

'''
rule make_contigs:
    version: "1.36.1"
    input:
        "MiSeq_SOP/{dataset}.files".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.fasta".format(dataset=dataset),
        "{dataset}.contigs.groups".format(dataset=dataset),
    threads: 1
    shell: 
        '''
        mothur "#make.contigs(file={input}, processors={threads})"
        '''

rule screen:
    version: "1.36.1"
    input:
        fasta = "{dataset}.trim.contigs.fasta".format(dataset=dataset),
        groups = "{dataset}.contigs.groups".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.fasta".format(dataset=dataset),
        "{dataset}.contigs.good.groups".format(dataset=dataset),
    shell:
        '''
        mothur "#screen.seqs(fasta={input.fasta}, group={input.groups}, maxambig={config[maxambig]}, maxlength={config[maxlength]})";
        '''
