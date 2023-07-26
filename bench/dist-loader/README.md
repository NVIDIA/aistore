# Distributed Loaders

This directory contains scripts and ansible playbooks to benchmark an AIS cluster using multiple hosts running [aisloader](/bench/aisloader), controlled by [ansible](https://github.com/ansible/ansible). To collect system metrics we optionally use [netdata](https://www.netdata.cloud/), a free and open source data aggregator deployed as a container on the AIS nodes.

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
2. Modify [start_netdata.sh](start_netdata.sh) and [start_grafana.sh](start_grafana.sh) to set the machine that will host grafana and graphite (if desired).
3. Modify the `run_playbook_*` scripts as needed to control object sizes, benchmark durations, etc. This also defines which hosts in your inventory run the aisloader scripts, which hosts are the benchmark targets (AIS proxies), and optionally which host runs grafana and graphite.
4. To run individual benchmarks, use the `run_playbook_*` scripts; to run all benchmarks use [run_all.sh](run_all.sh).

### Optional

1. To run disk benchmarks, uncomment the desired sections from the [disk_bench.sh](/bench/dist-loader/playbooks/scripts/disk_bench.sh) and run the `disk_bench.yaml` playbook with the `target_hosts` variable. This will trigger [fio](https://github.com/axboe/fio) benchmarks on the corresponding AIS targets.
2. To view [grafana](https://github.com/grafana/grafana) dashboards with metrics sent by `aisloader` and `netdata`, use your browser to access `grafana_host` (see `grafana_host` argument in the scripts). Note that the default grafana port is `3000`. Then you can optionally import the included throughput dashboard in `grafana_dashboards`.
3. To view individual host `netdata` dashboards, use your browser to access the host's IP at the `netdata` default port `19999`.


## Notes

 - The `get` benchmarks expect data to already exist in the clusters. Either populate the bucket or use the `put` benchmark first.
 - Note that individual `aisloader` hosts do not communicate with each other. Secondly, when running `put` workloads `aisloader` will create destination bucket _iff_ the latter does not exist. That's why it is recommended to create buckets _prior_ to writing (into those buckets) from multiple `aisloaders`.
 - None of the `aisloader` runs use the `cleanup` option. For all supported options, simply run `aisloader` or check [aisloader's readme](/docs/aisloader.md).
 - [fio](https://github.com/axboe/fio) `rand_write` is destructive and cannot (shall not!) be used in combination with the `allow_mounted_write` option. See https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-allow-mounted-write. The `rand_write` option is commented out in both the ansible playbook and the script.
   - see also: https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-allow-mounted-write
