#!/bin/bash
set -e

sudo systemctl start docker || true # Make sure docker is up...
sudo docker ps -a | grep -woP 'ddm-lmp-run.+' | sudo xargs -r docker rm -f

echo -e "\033[0;31m**Building the DDM-LMP Docker image**\033[0m"
sudo docker build . -t ddm-lmp

echo -e "\033[0;31m**Creating a subnet shared by all DDM-LMP containers**\033[0m"
if ! sudo docker network inspect ddm-lmp-net >/dev/null 2>/dev/null; then
    sudo docker network create --subnet=10.100.0.0/24 ddm-lmp-net
fi

echo -e "\033[0;31m**Spawning master and slave DDM-LMP containers with IP 10.100.0.100 and 10.100.0.101 **\033[0m"
sudo docker run --net ddm-lmp-net --ip "10.100.0.100" -d --name "ddm-lmp-run-master" ddm-lmp master -h 10.100.0.100
sudo docker run --net ddm-lmp-net --ip "10.100.0.101" -d --name "ddm-lmp-run-slave" ddm-lmp slave -mh 10.100.0.100 -h 10.100.0.101
