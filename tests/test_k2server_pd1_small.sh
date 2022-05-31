# Run from /mmfs1/groups/custflow/active/nhorner/kraken/code

python3 k2server.py \
  --ports 5555 5556 \
  --db /mmfs1/groups/custflow/active/nhorner/kraken/db \
  --threads 15 \
  --k2-binary /mmfs1/groups/custflow/active/nhorner/kraken_build/kraken2 &
server_pid=$!

client_pids=()
python3 k2client.py \
  --ports 5555 5556 \
  --fastq /mmfs1/groups/custflow/active/nhorner/bigdros/diffsizes/0.01/fastq_p0.01/subset_0.001.fq \
  --out drops1.tsv \
  --sample_id 001 &
client_pids+=($!)

python3 k2client.py \
  --ports 5555 5556 \
  --fastq /mmfs1/groups/custflow/active/nhorner/bigdros/diffsizes/0.01/fastq_p0.01/subset_0.001.fq \
  --out dros2.tsv \
  --sample_id 002
client_pids+=($!)

# wait for all pids
for pid in ${client_pids[*]}; do
    wait $pid
done

kill -9 $server_pid