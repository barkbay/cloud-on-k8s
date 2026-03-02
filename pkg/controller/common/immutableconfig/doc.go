// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

// Package immutableconfig provides utilities for managing immutable, content-addressed
// Kubernetes configuration resources (Secrets and ConfigMaps).
//
// The key idea is to use content-addressed naming: each configuration resource gets a
// name that includes a hash of its content (e.g., "my-config-a1b2c3d4"). This ensures
// that during rolling updates, old and new ReplicaSets reference different configuration
// resources, preventing race conditions where replacement pods might boot with the wrong
// configuration.
//
// # Usage
//
// Controllers using this package should:
//  1. Define a Classifier that maps config file names to Immutable or Dynamic
//  2. Use BuildImmutableSecret/BuildImmutableConfigMap to create content-addressed resources
//  3. Use ReconcileImmutableSecret/ReconcileImmutableConfigMap to create resources (create-only)
//  4. Use PatchSecretVolumes/PatchConfigMapVolumes to update pod template volumes
//  5. Use GCUnreferencedSecrets/GCUnreferencedConfigMaps to clean up old resources
//
// # Example
//
//	classifier := immutableconfig.MapClassifier{
//	    "config.yml": immutableconfig.Immutable,
//	    "dynamic.yml": immutableconfig.Dynamic,
//	}
//
//	immutableData, dynamicData, err := immutableconfig.SplitByClassification(allData, classifier)
//	if err != nil {
//	    return err
//	}
//
//	secret := immutableconfig.BuildImmutableSecret("my-config", namespace, immutableData, labels)
//	if err := immutableconfig.ReconcileImmutableSecret(ctx, client, secret); err != nil {
//	    return err
//	}
//
//	immutableconfig.PatchSecretVolumes(podSpec.Volumes, map[string]bool{"config-volume": true}, secret.Name)
package immutableconfig
