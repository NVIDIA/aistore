## Getting Started: Ansible

DFC can be run in bare VM or bare metal cluster. Here you can find simple ansible scripts that are meant to help configring DFC and start/stop it on Ubuntu base image. 

This README documents the steps to install and run DFC

#### Install Ansible and Parallel-ssh on controller node
https://community.spiceworks.com/how_to/110622-install-ansible-on-64-bit-ubuntu-14-04-lts
https://www.server-world.info/en/note?os=Ubuntu_14.04&p=ssh&f=8

#### Download dfc_ansible 

#### Configure DFC cluster
1. Create inventory file, see example inventory in dfc_ansible/inventory folder. 
2. Setup nodes - setup DFC paths, install go, aws cli
```
ansible-playbook -i inventory/cluster.ini setupnodes.yml
```
3. Get DFC - download DFC and related libs, build package
```
ansible-playbook -i inventory/cluster.ini getgdfc.yml
```
4. Config DFC - runs $DFCSRC/setup/config.sh to create dfc.json on all nodes in $HOME dir
```
ansible-playbook -i inventory/cluster.ini configdfc.yml
```
5. Copy helper scripts to start proxy/targets
```
ansible proxy -m copy -a "src=startproxy.sh dest=/home/ubuntu/startproxy.sh" -i inventory/cluster.ini --become
ansible proxy -m file -a "dest=/home/ubuntu/startproxy.sh mode=777 owner=ubuntu group=ubuntu" -i inventory/cluster.ini --become
ansible targets -m copy -a "src=starttarget.sh dest=/home/ubuntu/starttarget.sh" -i inventory/cluster.ini --become
ansible targets -m file -a "dest=/home/ubuntu/starttarget.sh mode=777 owner=ubuntu group=ubuntu" -i inventory/cluster.ini --become
```

6. Start DFC
```
parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/startproxy.sh >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
```

7. Stop DFC if needed
```
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/stopdfc.sh >/dev/null 2>&1'
parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/stopdfc.sh >/dev/null 2>&1'
```

8. Get logs from cluster when needed
```
ansible-playbook -i inventory/cluster.ini getdfclogs.yml
```

