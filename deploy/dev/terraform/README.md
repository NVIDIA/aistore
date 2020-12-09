---
layout: post
title: TERRAFORM
permalink: deploy/dev/terraform
redirect_from:
 - deploy/dev/terraform/README.md/
---

# Terraform - AIS GCP Playground

AIS can be run in bare VM or bare metal cluster. Here you can find simple `terraform` configuration which allows you to spin-up an Ubuntu VM on GCP with AIStore deployed. 

## Prerequisites
1. [Terraform (>= 0.12)](https://learn.hashicorp.com/tutorials/terraform/install-cli)
2. [GCP Project & Credentials](https://console.cloud.google.com/home/dashboard)
3. [Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html)

## Deployment

> **NOTE**: All commands should be executed from `deploy/dev/terraform` directory

### Setting variables (terraform.tfvars)
1. Get the GCP `json` credentials [service account key page](https://console.cloud.google.com/apis/credentials/serviceaccountkey). 
2. Update the `creds_file` variable to point to the downloaded `json` credentials file.
3. Set `project_id` to your GCP project ID.
4. Update the `ssh_private_file` and `ssh_public_file` to point to your `private` (e.g `~/.ssh/id_rsa`) and `public` (e.g `~/.ssh/id_rsa.pub`) keys. These are required to SSH into the deployed VM. If you wish to create a new ssh key pairs refer [this](https://www.ssh.com/ssh/keygen/)
5. Optionally, set the following variables:
    - `region` (default: `us-central1`) : GCP deployment region
    - `zone` (default: `us-central1-c`) : GCP deployment zone
    - `ansible_file` (default: `ansible/setupnodes.yml`) : ansible playbook for setting up AIStore on VM

#### Sample Configuration
Below is a sample variables file (terraform.tfvars)
```
creds_file       = "/home/ubuntu/creds/aistore-gcp.json"
project_id       = "aistore-29227"
ssh_private_file = "~/.ssh/gcp"
ssh_public_file  = "~/.ssh/gcp.pub"
```

### Useful commands

After updating the variables execute the below commands:

#### Initialize the terraform workspace
(to download the required provider plugins)
```console script
$ terraform init
Initializing the backend...

Initializing provider plugins...
- Using previously-installed hashicorp/google v3.5.0
- Using previously-installed hashicorp/null v2.1.2

Terraform has been successfully initialized!
$ 
```

#### Provisioning VM and deploying AIStore
(this might take a few minutes)
```console
$ terraform apply 
google_compute_network.vpc_network: Creating...
google_compute_network.vpc_network: Still creating... [10s elapsed]
google_compute_network.vpc_network: Still creating... [20s elapsed]
google_compute_network.vpc_network: Still creating... [30s elapsed]
google_compute_network.vpc_network: Still creating... [40s elapsed]
google_compute_network.vpc_network: Creation complete after 47s [id=projects/aistore-291017/global/networks/terraform-network]
google_compute_firewall.allow-ssh: Creating...
google_compute_instance.vm_instance: Creating...
google_compute_firewall.allow-ssh: Creation complete after 7s [id=projects/aistore-291017/global/firewalls/fw-allow-ssh]
google_compute_instance.vm_instance: Provisioning with 'remote-exec'...
.... TRUCATED ....
google_compute_instance.vm_instance (local-exec): Executing: ["/bin/sh" "-c" "ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -u ubuntu -i '<IP>,' --private-key /root/.ssh/gcp ansible/setupnodes.yml"]

google_compute_instance.vm_instance (local-exec): PLAY [all] *********************************************************************

google_compute_instance.vm_instance (local-exec): TASK [copy] ********************************************************************
google_compute_instance.vm_instance (local-exec): changed: [<IP>] => (item=setupnodes.sh)

google_compute_instance.vm_instance (local-exec): TASK [Execute the command in remote shell; stdout goes to the specified file on the remote.] ***

google_compute_instance.vm_instance (local-exec): PLAY RECAP *********************************************************************
google_compute_instance.vm_instance (local-exec): <IP>              : ok=2    changed=2    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0

google_compute_instance.vm_instance: Creation complete after 6m22s [id=projects/aistore-291017/zones/us-central1-c/instances/terraform-instance]

Apply complete! Resources: 3 added, 0 changed, 0 destroyed.

Outputs:

external_ip = <IP>
```

#### Getting external IP address
```console
$ terraform output
external_ip = <IP>
```

`ssh` into the VM to check the installation
```console
$ ssh -i <path-private-key> ubuntu@<EXTERNAL_IP>
```

#### Destroying the VM
```console
$ terraform destroy
```
