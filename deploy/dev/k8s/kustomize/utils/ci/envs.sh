#!/bin/bash

envsubst < ./overlays/ci/proxy/properties.env > ./overlays/ci/proxy/properties.env.tmp && mv ./overlays/ci/proxy/properties.env.tmp ./overlays/ci/proxy/properties.env
envsubst < ./overlays/ci/target/properties.env > ./overlays/ci/target/properties.env.tmp && mv ./overlays/ci/target/properties.env.tmp ./overlays/ci/target/properties.env
envsubst < ./overlays/ci/properties.env > ./overlays/ci/properties.env.tmp && mv ./overlays/ci/properties.env.tmp ./overlays/ci/properties.env