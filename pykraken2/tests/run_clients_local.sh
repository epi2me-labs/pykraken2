python3 client.py \
  --ports 5555 5556 \
  --fastq /Users/Neil.Horner/nf_dev/kraken/fastq/subset.fq \
  --out /Users/Neil.Horner/nf_dev/kraken/out/k2test_002_out.tsv \
  --sample_id 002 & \
python3 client.py \
  --ports 5555 5556 \
  --fastq /Users/Neil.Horner/nf_dev/kraken/fastq/subset2fq.fq \
  --out /Users/Neil.Horner/nf_dev/kraken/out/k2test_001_out.tsv \
  --sample_id 001;


#python3 client.py \
#  --ports 5555 5556 \
#  --fastq /Volumes/Groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_1.fastq \
#  --out 001.tsv \
#  --sample_id 002 & \
#python3 client.py \
#  --ports 5555 5556 \
#  --fastq /Volumes/Groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_2.fastq \
#  --out 002.tsv \
#  --sample_id 001;