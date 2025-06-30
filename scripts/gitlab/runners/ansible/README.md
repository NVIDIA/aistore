# AIStore GitLab Runner Re-Deployment

Simple Ansible playbook for re-deploying AIStore clusters on GitLab runners.

## Requirements

- [Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html) installed locally

## Setup

To set up key-based SSH access to the runners, copy your public key to the runner hosts:
   
```bash
ssh-copy-id <user>@10.131.128.103
ssh-copy-id <user>@10.131.128.104
```

## Usage

Configure the runner hosts in `hosts.ini`:
   
```ini
[runners]
ais-etl-gitlab-runner-00 ansible_host=10.131.128.103
ais-etl-gitlab-runner-01 ansible_host=10.131.128.104
```

Export DockerHub credentials:

```bash
export DOCKERHUB_USERNAME=<dockerhub_username>
export DOCKERHUB_TOKEN=<dockerhub_token>
```

Then, run the playbook:

```bash
# Deploy to all runners
ansible-playbook -i hosts.ini redeploy.yml

# Deploy to specific runner
ansible-playbook -i hosts.ini redeploy.yml --limit ais-etl-gitlab-runner-00
```
