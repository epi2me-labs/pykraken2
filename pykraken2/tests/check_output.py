import pandas as pd
from collections import Counter
# df = pd.read_csv(
#     '/Users/Neil.Horner/nf_dev/kraken/debug.tsv', sep='\t'
# )

# print(df)

# should be 500/502 results lines, getting 726
ids = []
with open('/Volumes/Groups/custflow/active/nhorner/kraken/code/SRR17237156_1.tsv') as fh1, open('/Volumes/Groups/custflow/active/nhorner/kraken/code/SRR17237156_2.tsv') as fh2:
    for i, (l1, l2) in enumerate(zip(fh1, fh2)):
        # ids.append(line.split('\t')[1])
        id1 = l1.split('\t')[1]
        id2 = l2.split('\t')[1]
        if id1 != id1:
            print(i, id1, id2)
            break
    ...

# print(len(ids))
# c = Counter(ids)
# print(max(list(c.values())))



