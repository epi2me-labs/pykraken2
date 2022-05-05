import pandas as pd
from collections import Counter
# df = pd.read_csv(
#     '/Users/Neil.Horner/nf_dev/kraken/debug.tsv', sep='\t'
# )

# print(df)

# should be 500/502 results lines, getting 726
ids = []
with open('/Users/Neil.Horner/nf_dev/kraken/debug.tsv') as fh:
    for line in fh:
        ids.append(line.split('\t')[1])

print(len(ids))
c = Counter(ids)
print(max(list(c.values())))



