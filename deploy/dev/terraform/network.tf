# VPC
resource "google_compute_network" "vpc_network" {
    name = "${var.project_id}-network"
}

resource "google_compute_firewall" "allow-ssh" {
    name = "${var.project_id}-allow-ssh"
    network = google_compute_network.vpc_network.name
    allow {
        protocol = "tcp"
        ports    = ["22"]
    }
    target_tags = ["ssh"]
    source_ranges = ["0.0.0.0/0"]
}
