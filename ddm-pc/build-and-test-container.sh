#!/bin/bash
set -e

N_SLAVES=2

podman ps -a | grep -woP 'ddm-pc-run.+' | xargs -r podman rm -f

echo -e "\033[0;31m**Building the DDM-PC container image**\033[0m"
podman build . -t ddm-pc

echo -e "\033[0;31m**Creating a subnet shared by all DDM-PC containers**\033[0m"
if ! podman pod inspect ddm-pc-pod >/dev/null 2>/dev/null; then
    podman pod create --name ddm-pc-pod
fi

echo -e "\033[0;31m**Spawning master and slave DDM-PC containers **\033[0m"
podman run --pod ddm-pc-pod -d --name "ddm-pc-run-master" ddm-pc master -h localhost

for i in $(seq 1 $N_SLAVES); do
	port=$(( 2 * $i + 10000 ))
	podman run --pod ddm-pc-pod -d --name "ddm-pc-run-slave$i" ddm-pc slave -mh localhost -h localhost -p "$port"
done