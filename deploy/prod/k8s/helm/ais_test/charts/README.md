# AIS TEST helm chart

## Overview

This repo includes all the definitions of launching a test against an existing AIS cluster.
It runs the following tests:
  1. Verify that the expected number of targets are running.
  2. Upload objects to a local bucket in the cluster for the specified duration
  3. Perform list objects operation against the bucket created in the earlier step and verify its existence


```bash

TO INSTALL
==========
Usage: helm install --name=ais-test --set test_params.proxy_endpoint=<proxy-svc-endpoint> .


TO DESTROY
==========
Usage: helm del --purge ais-test


```
