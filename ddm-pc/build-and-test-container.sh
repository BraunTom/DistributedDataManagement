#!/bin/bash
set -e

N_SLAVES=2

sudo docker ps -a | grep -woP 'ddm-pc-run.+' | xargs -r sudo docker rm -f

echo -e "\033[0;31m**Building the DDM-PC container image**\033[0m"
sudo docker build . -t ddm-pc

echo -e "\033[0;31m**Creating a subnet shared by all DDM-PC containers**\033[0m"
if ! sudo docker network inspect ddm-pc-net >/dev/null 2>/dev/null; then
    sudo docker network create --subnet=10.100.1.0/24 ddm-pc-net
fi

echo -e "\033[0;31m**Spawning master and slave DDM-PC containers **\033[0m"
sudo docker run --net ddm-pc-net --ip "10.100.1.100" -d --name "ddm-pc-run-master" ddm-pc master -h 10.100.1.100

for i in $(seq 1 $N_SLAVES); do
	ip=$(( $i + 100))
	sudo docker run --net ddm-pc-net --ip "10.100.1.$ip" -d --name "ddm-pc-run-slave$i" ddm-pc slave -mh 10.100.1.100 -h "10.100.1.$ip"
done
