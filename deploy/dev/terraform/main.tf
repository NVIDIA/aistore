terraform {
    required_providers {
        google = {
          source = "hashicorp/google"
        }
    }
}

variable "project_id" {
    type = string
    description = "project id"
}

variable "region" {
    type = string
    default = "us-central1"
    description = "region"
}

variable "creds_file" {
    type = string
    description = "credentials json file"
}

variable "ssh_public_file" {
    type = string
    description = "path to public key file"
}

variable "ssh_private_file" {
    type = string
    description = "path to private key file"
}

variable "zone" {
    type = string
    default = "us-central1-c"
    description = "zone"
}

variable "ansible_file" {
    type = string
    default = "../../prod/ansible/setupnodes.yml"
    description = "path to ansible config (.yml) file"
}

provider "google" {
    version = "3.5.0"
    credentials = file(var.creds_file)
    project = var.project_id
    region  = var.region
    zone    = var.zone
}

resource "google_compute_instance" "vm_instance" {
    name         = "${var.project_id}-instance"
    machine_type = "g1-small"
    scheduling {
        preemptible       = true
        automatic_restart = false
    }

    boot_disk {
        initialize_params {
          image = "ubuntu-1804-bionic-v20200317"
        }
    }
    tags = ["ssh"]

    metadata = {
        ssh-keys = "ubuntu:${file(var.ssh_public_file)}"
    }

    network_interface {
        network = google_compute_network.vpc_network.name
        access_config {
        }
    }
}

# Ansible AIStore deployment
resource "null_resource" "ansible" {
    depends_on = [google_compute_instance.vm_instance]

    connection {
        user = "ubuntu"
        private_key = file(var.ssh_private_file)
        host = google_compute_instance.vm_instance.network_interface.0.access_config.0.nat_ip
    }

    # ensure instance is ready
    provisioner "remote-exec" {
        script = "scripts/wait_for_instance.sh"
    }

    provisioner "local-exec" {
         command = "ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -u ubuntu -i '${google_compute_instance.vm_instance.network_interface.0.access_config.0.nat_ip},' --private-key ${var.ssh_private_file} ${var.ansible_file}"
    }

    provisioner "remote-exec" {
        script = "scripts/deploy_ais.sh"
    }
}

output "external_ip" {
    value = google_compute_instance.vm_instance.network_interface.0.access_config.0.nat_ip
    description = "external ip address of the instance"
}