## Distributed Loaders

This directory contains scripts and ansible playbooks to benchmark an AIS cluster using multiple hosts running aisloader, controlled by ansible. For collecting system metrics it uses [netdata](https://www.netdata.cloud/), a free and open source data aggregator deployed as a container on the AIS nodes. 


### Prerequisites: 
 
- python3 and pip3 available in your path
- Ansible installed: `python3 -m pip install --user ansible`
- Aisloader installed on each of the hosts it will run on
    - You can use ansible to copy the binary to each of the loader nodes, e.g. `ansible <name of loader hosts section in inventory> -m ansible.builtin.copy -a "src=/path/to/aisloader dest=/usr/local/bin/aisloader" -i inventory.yaml`
- Network access to each of the aisloader hosts
- Network access from each of the aisloader hosts to the AIS proxy
- If using netdata for metrics, docker must be installed on each target node. This can be done with the provided `install_docker.sh`.


### Configuration:

1. Set up an ansible hosts configuration file (the current scripts all use `inventory.yaml`). This file must have a section `aisloader_hosts` and a section `target_hosts`.
2. Modify `start_netdata.sh` and `start_grafana.sh` to set the machine that will host grafana and graphite (if desired).
3. Modify the `run_playbook_*` scripts as needed to control object sizes, benchmark durations, etc. This also defines which hosts in your inventory run the aisloader scripts, which hosts are the benchmark targets (AIS proxies), and optionally which host runs grafana and graphite.
4. To run individual benchmarks, use the `run_playbook_*` scripts or to run all benchmarks use `run_all.sh`.

 Optional
1. To run disk benchmarks, uncomment the desired sections from disk_bench.sh and run the `disk_bench.yaml` playbook with the `target_hosts` variable to trigger fio benchmarks on the targets and extract results. 
2. To view grafana dashboards with metrics from aisloader and netdata, use your browser to access the host provided with the `grafana_host` argument in the scripts at the grafana port (default 3000). Then you can optionally import the included throughput dashboard in `grafana_dashboards`.
3. To view individual host netdata dashboards, use your browser to access the host's ip at the netdata default port, 19999. 


### Notes:

 - The `get` benchmarks expect data to already exist in the clusters, so either populate the bucket or use the `put` benchmark first.
 - The `put` benchmarks sometimes hang if the bucket is not already created, as the individual aisloader hosts do not communicate and may try to all create the same bucket.
 - None of the aisloader runs use the `cleanup` option.
 - fio `rand_write` is destructive and cannot be run with the allow_mounted_write option (and sometimes not then). See https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-allow-mounted-write. It is commented out in both the ansible playbook and the script to avoid any accidental runs. 
