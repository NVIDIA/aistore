ps -C dfc -o pid= | xargs sudo kill -9
sudo /home/ubuntu/dfc/bin/dfc -config=/home/ubuntu/dfc.json -role=target &

