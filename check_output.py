import pandas as pd

df = pd.read_csv(
    '/Volumes/Groups/custflow/active/nhorner/kraken/code/SRR17237156_1.tsv'
)


with open('/Volumes/Groups/custflow/active/nhorner/kraken/code/SRR17237156_1.tsv') as fh, open('/Users/Neil.Horner/nf_dev/kraken') as fho:
    for line in fh:

