package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/elastic/cloud-on-k8s/hack/deployer/runner"
)

func main() {
	log.Println("Authenticating as service account...")

	gcpDir := filepath.Join("/Users/michael/SUPPORT/e2e/gke-broken", runner.GCPDir)
	log.Printf("Setting CLOUDSDK_CONFIG=%s", gcpDir)
	if err := os.Setenv("CLOUDSDK_CONFIG", gcpDir); err != nil {
		panic(err)
	}
	if err := runner.NewCommand(fmt.Sprintf("echo CLOUDSDK_CONFIG is set to ${CLOUDSDK_CONFIG}")).Run(); err != nil {
		panic(err)
	}

	keyFileName := filepath.Join(gcpDir, runner.ServiceAccountFilename)
	// now that we're set on the cloud sdk directory, we can run any gcloud command that will rely on it
	if err := runner.NewCommand(fmt.Sprintf("gcloud config set project %s", "elastic-cloud-dev")).Run(); err != nil {
		panic(err)
	}

	if err := runner.NewCommand("gcloud auth activate-service-account --key-file=" + keyFileName).Run(); err != nil {
		panic(err)
	}
}
