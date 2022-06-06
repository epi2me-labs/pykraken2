#time kraken2 \
#--report 'kraken2_report.txt' \
#--classified-out CLASSIFIED_READS \
#--unclassified-out UNCLASSIFIED_READS \
#--db /mmfs1/groups/custflow/active/sgriffiths/metagenomics/all_refs/pig_sheep_cow_horse/kraken_db \
#--threads 40 \
#/mmfs1/groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_1.fastq > \
#SRR17237156_1_just_kraken.tsv;
#
#kraken2 \
#--report 'kraken2_report.txt' \
#--classified-out CLASSIFIED_READS \
#--unclassified-out UNCLASSIFIED_READS \
#--db /mmfs1/groups/custflow/active/sgriffiths/metagenomics/all_refs/pig_sheep_cow_horse/kraken_db \
#--threads 40 \
#/mmfs1/groups/custflow/active/nhorner/kraken/speed_comp/SRR17237156_1.fastq > \
#SRR17237156_1_just_kraken.tsv;

# Compare original kraken with realtime kraken

kraken2 \
--report 'kraken2_report.txt' \
--classified-out CLASSIFIED_READS \
--unclassified-out UNCLASSIFIED_READS \
--db /mmfs1/groups/custflow/active/sgriffiths/metagenomics/all_refs/pig_sheep_cow_horse/kraken_db \
--threads 40 \
/dev/fd/0 > \
raw_kraken.tsv;