#!/bin/bash
set -e

N_SLAVES=2

sudo docker ps -a | grep -woP 'ddm-lmp-run.+' | xargs -r sudo docker rm -f

echo -e "\033[0;31m**Building the DDM-LMP container image**\033[0m"
sudo docker build . -t ddm-lmp

echo -e "\033[0;31m**Creating a subnet shared by all DDM-LMP containers**\033[0m"
if ! sudo docker network inspect ddm-lmp-net >/dev/null 2>/dev/null; then
    sudo docker network create --subnet=10.100.0.0/24 ddm-lmp-net
fi

echo -e "\033[0;31m**Spawning master and slave DDM-LMP containers **\033[0m"
sudo docker run --net ddm-lmp-net --ip "10.100.0.100" -d --name "ddm-lmp-run-master" ddm-lmp master -h 10.100.0.100

for i in $(seq 1 $N_SLAVES); do
	ip=$(( $i + 100))
	sudo docker run --net ddm-lmp-net --ip "10.100.0.$ip" -d --name "ddm-lmp-run-slave$i" ddm-lmp slave -mh 10.100.0.100 -h "10.100.0.$ip"
done
