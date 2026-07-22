# Security Policy: AIStore

## Supported Versions

Security updates and support are provided for the latest and most recent AIStore releases. Keep deployments on the latest stable release.

| Version        | Supported          |
| -------------- | ------------------ |
| Latest release | :white_check_mark: |
| `main` branch  | :white_check_mark: |
| Older versions | :x:                |

## Reporting a Vulnerability

Do not open a public issue or pull request. Report vulnerabilities privately using one of these methods:

1. **NVIDIA Vulnerability Disclosure Program (preferred):** Submit the [official NVIDIA vulnerability report](https://www.nvidia.com/en-us/security/report-vulnerability/).
2. **GitHub private vulnerability reporting:** Open this repository's **Security** tab and select **Report a vulnerability**.
3. **Email:** Send vulnerability details to [psirt@nvidia.com](mailto:psirt@nvidia.com) and copy [aistore@nvidia.com](mailto:aistore@nvidia.com) for AIStore-specific coordination.

Include the affected version or branch, vulnerability type, reproduction steps, proof of concept if available, and expected impact. NVIDIA PSIRT will acknowledge, assess, and coordinate remediation and disclosure.

## Security Architecture and Context

AIStore is a distributed object-storage service with native HTTP and S3-compatible APIs, proxy and target nodes, remote cloud backends, and command-line and SDK clients. Its main security boundaries are client-facing APIs, intra-cluster networks, storage hosts, and remote backends.

**Repository Exposure Classification:** Public. Basis: published in a public NVIDIA GitHub repository.

**Service Exposure Classification:** External / Regulated (high confidence). Basis: externally distributed production storage software and client APIs.

### Threat Model

1. **Unauthorized data or administrative access:** AIStore APIs manage objects, buckets, cluster membership, and batch operations. Missing or incorrect JWT, TLS, or network controls can expose data or privileged actions.
2. **Malicious or oversized input:** Object, archive, S3, and batch APIs process caller-controlled names, metadata, and payloads. Crafted requests can target parsers or exhaust network, memory, CPU, or storage resources.
3. **Compromised cluster member:** Proxies and targets exchange control, metadata, and object traffic. A compromised node or host can affect cluster integrity, confidentiality, or availability.
4. **Backend credential exposure:** Cloud backends use external credentials and endpoints. Leaked or over-privileged credentials can expose remote data beyond the AIS cluster.

### Critical Security Assumptions

- Operators correctly configure TLS, JWT authentication and authorization, firewalls, and public versus intra-cluster networks.
- Cluster hosts, Kubernetes, storage devices, and the container runtime enforce their isolation and access controls.
- Cloud credentials and TLS private keys are protected and limited to the access required by the deployment.
- Only trusted administrators can perform destructive cluster operations or initialize ETL workloads.
