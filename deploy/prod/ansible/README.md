---
layout: post
title: ANSIBLE
permalink: deploy/prod/ansible
redirect_from:
 - deploy/prod/ansible/README.md/
---

## Getting Started: Ansible

AIS can be run in bare VM or bare metal cluster. Here you can find simple ansible scripts that are meant to help configure AIS and start/stop it on Ubuntu base image.

Ansible is used mostly to install few modules and copy simple AIS helper bash scripts over to AIS cluster. Most of the AIS operations are done using parallel-ssh.

This README documents the steps to install and run AIS

#### Install Ansible and Parallel-ssh on controller node
https://community.spiceworks.com/how_to/110622-install-ansible-on-64-bit-ubuntu-14-04-lts
https://www.server-world.info/en/note?os=Ubuntu_14.04&p=ssh&f=8

#### Download 

```console
$ git clone git://github.com/NVIDIA/aistore
$ git checkout ais_ansible
```

#### Configure AIS cluster
1. Create inventory file, see example inventory in ais_ansible/inventory folder.
2. Setup nodes - setup AIS paths, install go, aws cli

```console
$ ansible-playbook -i inventory/cluster.ini setupnodes.yml
```

3. Get AIS - download AIS and related libs, build package

```console
$ ansible-playbook -i inventory/cluster.ini getgais.yml
```

4. Config AIS - runs $AISSRC/setup/config.sh to create ais.json on all nodes in $HOME dir

```console
$ ansible-playbook -i inventory/cluster.ini configais.yml
```

5. Copy helper scripts to start proxy/targets

```console
$ ansible proxy -m copy -a "src=startproxy.sh dest=/home/ubuntu/startproxy.sh" -i inventory/cluster.ini --become
$ ansible proxy -m file -a "dest=/home/ubuntu/startproxy.sh mode=777 owner=ubuntu group=ubuntu" -i inventory/cluster.ini --become
$ ansible targets -m copy -a "src=starttarget.sh dest=/home/ubuntu/starttarget.sh" -i inventory/cluster.ini --become
$ ansible targets -m file -a "dest=/home/ubuntu/starttarget.sh mode=777 owner=ubuntu group=ubuntu" -i inventory/cluster.ini --become
```

6. Start AIS

```console
$ parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/startproxy.sh >/dev/null 2>&1'
$ parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
```

7. Stop AIS if needed

```console
$ parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/stopais.sh >/dev/null 2>&1'
$ parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/stopais.sh >/dev/null 2>&1'
```

8. Get logs from cluster when needed

```console
$ ansible-playbook -i inventory/cluster.ini getaislogs.yml
```
