// Package main contains the independent authentication server for AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/authn/config"
	"github.com/NVIDIA/aistore/cmd/authn/kvdb"
	"github.com/NVIDIA/aistore/cmd/authn/signing"
	"github.com/NVIDIA/aistore/cmd/authn/tok"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

var (
	build     string
	buildtime string
)

func logFlush(interval time.Duration) {
	for {
		time.Sleep(interval)
		nlog.Flush(nlog.ActNone)
	}
}

func main() {
	cfgPath := flag.String("config", "", config.ServiceName+" configuration")
	showVer := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVer || len(os.Args) == 2 && os.Args[1] == "version" {
		printVer()
		os.Exit(0)
	}
	if shouldPrintHelp() {
		printHelp()
		os.Exit(0)
	}
	installSignalHandler()
	cm := config.NewConfManager()
	cm.Init(*cfgPath)
	driver := kvdb.CreateDriver(cm)
	// Initialize the interface used to sign tokens and validate key signatures
	signer := initSigner(cm)
	mgr, code, err := newMgr(cm, signer, driver)
	if err != nil {
		cos.ExitLogf("Failed to init manager: %v(%d)", err, code)
	}

	nlog.Infof("Version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)

	// Flush all init logs immediately
	nlog.Flush(nlog.ActNone)
	go logFlush(cm.GetLogFlushInterval())

	srv := newServer(mgr)
	err = srv.Run()

	nlog.Flush(nlog.ActExit)
	cos.Close(mgr.db)
	if err != nil {
		cos.ExitLogf("Server failed: %v", err)
	}
}

func installSignalHandler() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		os.Exit(0)
	}()
}

func shouldPrintHelp() bool {
	_, found := os.LookupEnv(env.AisAuthConfDir)
	if len(os.Args) == 1 && !found {
		return true
	}
	return len(os.Args) == 2 && strings.Contains(os.Args[1], "help")
}

func printHelp() {
	printVer()
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Printf("  %s -config <path>\n", os.Args[0])
	fmt.Println()
	fmt.Println("  For initial setup, you must set the " + env.AisAuthAdminPassword + " environment variable.")
	fmt.Println()
	fmt.Println("DESCRIPTION:")
	fmt.Println("  AIS Authentication Server (AuthN) provides token-based secure access to AIStore.")
	fmt.Println("  It uses JSON Web Tokens (JWT) to grant access to buckets and objects.")
	fmt.Println()
	fmt.Println("OPTIONS:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("ENVIRONMENT VARIABLES:")
	fmt.Println("  All environment variables override corresponding config file values when set.")
	fmt.Println()
	fmt.Println("  Configuration:")
	fmt.Printf("    %-30s  %s\n", env.AisAuthConfDir, "Config directory (alternative to -config)")
	fmt.Printf("    %-30s  %s\n", env.AisAuthPort, "Server port")
	fmt.Printf("    %-30s  %s\n", env.AisAuthUseHTTPS, "Enable HTTPS (true/false)")
	fmt.Printf("    %-30s  %s\n", env.AisAuthServerCrt, "HTTPS certificate file")
	fmt.Printf("    %-30s  %s\n", env.AisAuthServerKey, "HTTPS private key file")
	fmt.Printf("    %-30s  %s\n", env.AisAuthExternalURL, "External URL for OIDC discovery")
	fmt.Printf("    %-30s  %s\n", env.AisAuthLogDir, "Log directory")
	fmt.Println()
	fmt.Println("  Authentication:")
	fmt.Printf("    %-30s  %s\n", env.AisAuthAdminPassword, "Admin user password (REQUIRED)")
	fmt.Printf("    %-30s  %s\n", env.AisAuthAdminUsername, "Admin username (default: 'admin')")
	fmt.Printf("    %-30s  %s\n", env.AisAuthSecretKey, "HMAC secret key (overrides config)")
	fmt.Printf("    %-30s  %s\n", env.AisAuthPrivateKeyFile, "RSA private key file path")
	fmt.Printf("    %-30s  %s\n", env.AisAuthPrivateKeyPass, "RSA private key passphrase (optional)")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Printf("  %s -config /etc/authn\n", os.Args[0])
	fmt.Printf("  %s -config /etc/authn/authn.json\n", os.Args[0])
	fmt.Println()
	fmt.Println("CONFIGURATION:")
	fmt.Println("  The configuration file must be named 'authn.json' and can be specified via:")
	fmt.Println("  - The -config flag (file or directory path)")
	fmt.Println("  - The " + env.AisAuthConfDir + " environment variable")
	fmt.Println()
	fmt.Println("  See docs/authn.md for detailed configuration options.")
}

func printVer() {
	fmt.Printf("version %s (build %s)\n", cmn.VersionAuthN+"."+build, buildtime)
}

func initSigner(cm *config.ConfManager) tok.Signer {
	if hmac := cm.GetSecret(); hmac != "" {
		return signing.NewHMACSigner(hmac)
	}
	nlog.Infof("No HMAC secret provided via config or %q, initializing with RSA", env.AisAuthSecretKey)
	return initRSA(cm)
}

func initRSA(cm *config.ConfManager) *signing.RSAKeyManager {
	passphrase, err := validatePassphrase()
	if err != nil {
		cos.ExitLogf("Failed RSA key passphrase validation for %s: %v", env.AisAuthPrivateKeyPass, err)
	}
	rsaMgr := signing.NewRSAKeyManager(cm.GetRSAConfig(), passphrase)
	if err = rsaMgr.Init(); err != nil {
		cos.ExitLogf("Failed to initialize RSA key: %v", err)
	}
	return rsaMgr
}

func validatePassphrase() (cmn.Censored, error) {
	passphrase, found := os.LookupEnv(env.AisAuthPrivateKeyPass)
	if !found {
		return "", nil
	}
	if len(passphrase) < signing.MinPassphraseLength {
		return "", fmt.Errorf("too short (must be at least %d characters)", signing.MinPassphraseLength)
	}
	if cos.Entropy(passphrase) < signing.MinPassphraseEntropy {
		return "", errors.New("passphrase strength too low. Try increasing length or complexity")
	}
	disallowed := "\t\n\r"
	if strings.ContainsAny(passphrase, disallowed) {
		return "", fmt.Errorf("cannot contain whitespace escape sequences: %q", disallowed)
	}
	return cmn.Censored(passphrase), nil
}
