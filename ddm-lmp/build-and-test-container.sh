#!/bin/bash
set -e

N_SLAVES=2

podman ps -a | grep -woP 'ddm-lmp-run.+' | xargs -r podman rm -f

echo -e "\033[0;31m**Building the DDM-LMP container image**\033[0m"
podman build . -t ddm-lmp

echo -e "\033[0;31m**Creating a subnet shared by all DDM-LMP containers**\033[0m"
if ! podman pod inspect ddm-lmp-pod >/dev/null 2>/dev/null; then
    podman pod create --name ddm-lmp-pod
fi

echo -e "\033[0;31m**Spawning master and slave DDM-LMP containers **\033[0m"
podman run --pod ddm-lmp-pod -d --name "ddm-lmp-run-master" ddm-lmp master -h localhost

for i in $(seq 1 $N_SLAVES); do
	port=$(( 2 * $i + 10000 ))
	podman run --pod ddm-lmp-pod -d --name "ddm-lmp-run-slave$i" ddm-lmp slave -mh localhost -h localhost -p "$port"
done