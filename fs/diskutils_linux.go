/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package fs

import (
	"fmt"
	"os/exec"
	"strings"
)

// NOTE: Since this invokes a shell command, it is slow.
// Do not use this in code paths which are executed per object.
// This method is used only at startup to store the file systems
// for each mount path.
func Fqn2fsAtStartup(fqn string) (string, error) {
	getFSCommand := fmt.Sprintf("df -P '%s' | awk 'END{print $1}'", fqn)
	outputBytes, err := exec.Command("sh", "-c", getFSCommand).Output()
	if err != nil || len(outputBytes) == 0 {
		return "", fmt.Errorf("unable to retrieve FS from fspath %s, err: %v", fqn, err)
	}

	return strings.TrimSpace(string(outputBytes)), nil
}
