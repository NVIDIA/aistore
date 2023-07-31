# AISLoader Composer

This directory contains scripts and ansible playbooks to benchmark an AIS cluster using multiple hosts running [aisloader](/bench/tools/aisloader), controlled by [ansible](https://github.com/ansible/ansible). To collect system metrics we optionally use [netdata](https://www.netdata.cloud/), a free and open source data aggregator deployed as a container on the AIS nodes.

## Prerequisites

- python3 and pip3 available in your path
- [ansible](https://github.com/ansible/ansible) installed: `python3 -m pip install --user ansible`
- [aisloader](/docs/aisloader.md) installed on each of the hosts it will run on
    - use `ansible` to copy the binary to each of the loader nodes, e.g.:
```console
ansible <name of loader hosts section in inventory> -m ansible.builtin.copy -a "src=/path/to/aisloader dest=/usr/local/bin/aisloader" -i inventory.yaml
```
- network access to each of the `aisloader` hosts
- network access from each of the `aisloader` hosts to any AIS proxy in the cluster
- if [netdata](https://www.netdata.cloud/) is used, docker must be installed on each AIS target node. Use provided [install_docker.sh](install_docker.sh) script.

## Configuration

1. Set up an ansible hosts configuration file (the current scripts all use [inventory.yaml](inventory.yaml)). This file must have a section `aisloader_hosts` and a section `target_hosts`.
2. Set up stats monitoring
  1. Modify [start_netdata.sh](start_netdata.sh) and [start_grafana.sh](start_grafana.sh) to set the machine that will host grafana and graphite.
  2. Ensure docker is installed and accessible on each target machine. The provided `install_docker.sh` will do this automatically.
  3. Run `deploy_grafana.sh` and `start_netdata.sh` to start the containers to collect and display aisloader and system statistics.
3. Configure your benchmarks
  1. Modify the `run_playbook_*` scripts as needed to set object sizes, benchmark durations, bucket names, and graphite/grafana host.
  2. Configure the number of worker threads each aisloader instance will use in [playbooks/vars/bench.yaml](/bench/tools/aisloader-composer/playbooks/vars/bench.yaml).
4. Run `configure_aisloader.sh` to update the TCP settings on the aisloader hosts. This is necessary to enable a very large number of outbound connections (to the AIS cluster) without exhausting the number of local ports available. 
5. To run individual benchmarks, use the `run_playbook_*` scripts; to run all benchmarks use [run_all.sh](run_all.sh).

### Optional

1. To run disk benchmarks, uncomment the desired sections from the [disk_bench.sh](/bench/aisloader-composer/playbooks/scripts/disk_bench.sh) and run the `disk_bench.yaml` playbook with the `target_hosts` variable. This will trigger [fio](https://github.com/axboe/fio) benchmarks on the corresponding AIS targets.
2. To view [grafana](https://github.com/grafana/grafana) dashboards with metrics sent by `aisloader` and `netdata`, use your browser to access `grafana_host` (see `grafana_host` argument in the scripts). Note that the default grafana port is `3000`. Then you can optionally import the included throughput dashboard in `grafana_dashboards`.
3. To view individual host `netdata` dashboards, use your browser to access the host's IP at the `netdata` default port `19999`.


## Notes

 - The `get` benchmarks expect data to already exist in the clusters. Either populate the bucket or use the `put` benchmark first.
 - Note that individual `aisloader` hosts do not communicate with each other. Secondly, when running `put` workloads `aisloader` will create destination bucket _iff_ the latter does not exist. That's why it is recommended to create buckets _prior_ to writing (into those buckets) from multiple `aisloaders`.
 - None of the `aisloader` runs use the `cleanup` option. For all supported options, simply run `aisloader` or check [aisloader's readme](/docs/aisloader.md).
 - [fio](https://github.com/axboe/fio) `rand_write` is destructive and cannot (shall not!) be used in combination with the `allow_mounted_write` option. The `rand_write` option is commented out in both the ansible playbook and the script.
   - see also: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-allow-mounted-write
