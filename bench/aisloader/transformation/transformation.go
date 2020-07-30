// Package transformation
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transformation

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/transform"
)

const (
	Tar2TF = "tar2tf"
	Echo   = "echo"
	Md5    = "md5"
)

var (
	links = map[string]string{
		Md5:    "https://raw.githubusercontent.com/NVIDIA/ais-tar2tf/master/transformers/md5/pod.yaml",
		Tar2TF: "https://raw.githubusercontent.com/NVIDIA/ais-tar2tf/master/transformers/tar2tf/pod.yaml",
		Echo:   "https://raw.githubusercontent.com/NVIDIA/ais-tar2tf/master/transformers/echo/pod.yaml",
	}

	client = &http.Client{}
)

func validateTransformName(name string) error {
	if _, ok := links[name]; !ok {
		return fmt.Errorf("%s not a valid transform name; expected one of %s, %s, %s", name, Echo, Tar2TF, Md5)
	}
	return nil
}

func GetYaml(name string) ([]byte, error) {
	if err := validateTransformName(name); err != nil {
		return nil, err
	}

	resp, err := client.Get(links[name])
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(b))
	}

	specStr := os.Expand(string(b), func(v string) string {
		// Hack: Neither os.Expand, nor os.ExpandEnv supports bash env variable default-value
		// syntax. The whole ${VAR:-default} is matched as v.
		if strings.Contains(v, "COMMUNICATION_TYPE") {
			return transform.RedirectCommType
		}
		if strings.Contains(v, "DOCKER_REGISTRY_URL") {
			return "aistore"
		}
		return ""
	})

	return []byte(specStr), nil
}
