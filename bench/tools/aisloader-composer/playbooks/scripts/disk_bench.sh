#!/bin/bash
hostname=$(hostname -a)
sudo mkdir /tmp/diskbench/
rm -rf $outfile


# filename="fio-randread"
# outfile="/tmp/diskbench/$filename-$hostname.json"
# sudo fio rand_read.fio --output-format=json --output=$outfile

# filename="fio-randwrite"
# outfile="/tmp/diskbench/$filename-$hostname.json"
# sudo fio rand_write.fio --output-format=json --output=$outfile

# filename="fio-seqread"
# outfile="/tmp/diskbench/$filename-$hostname.json"
# sudo fio seq_read.fio --output-format=json --output=$outfile
