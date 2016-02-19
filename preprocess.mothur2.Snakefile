'''
Preprocess results using trimmomatic

Note -- this obviously doesn't use trimmomatic but is an example of how
        one could swap out one tool for another

'''
rule make_contigs_screen_trimmomatic:
    version: "1.36.1"
    input:
        "{dataset}.files".format(dataset=dataset),
    output:
        "{dataset}.trim.contigs.good.fasta".format(dataset=dataset),
        "{dataset}.contigs.good.groups".format(dataset=dataset),
    threads: 1
    shell: 
        '''
        mothur "#make.contigs(file={input}, processors={threads});
                screen.seqs(fasta=current, group=current, maxambig={config[maxambig]}, maxlength={config[maxlength]})"
        '''

'''
Full output from make_contigs:
stability.trim.contigs.qual
stability.contigs.report
stability.scrap.contigs.fasta
stability.scrap.contigs.qual
stability.contigs.groups
'''
