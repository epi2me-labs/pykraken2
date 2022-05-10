# Run from /mmfs1/groups/custflow/active/nhorner/kraken/code

python3 k2server.py \
--ports 5555 5556 \
--db /mmfs1/groups/custflow/active/sgriffiths/metagenomics/all_refs/pig_sheep_cow_horse/kraken_db & \
python3 k2client.py \
  --ports 5555 5556 \
  --fastq /mmfs1/groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_1.fastq \
  --out SRR17237156_1.tsv \
  --sample_id 001 & \
python3 k2client.py \
  --ports 5555 5556 \
  --fastq /mmfs1/groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_1.fastq \
  --out SRR17237156_2.tsv \
  --sample_id 002;