INVENTORY="inventory.yaml"

ansible-playbook -i $INVENTORY playbooks/clear_cache.yaml -f 10 --become -e "ansible_become_pass=y"
