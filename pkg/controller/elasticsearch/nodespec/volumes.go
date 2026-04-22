// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package nodespec

import (
	corev1 "k8s.io/api/core/v1"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/volume"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/filesettings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/initcontainer"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/settings"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/user"
	esvolume "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/volume"
)

// buildVolumes assembles the volumes and volume mounts for an Elasticsearch pod.
//
// The function branches on es.IsStateless() for the data volume:
//   - Stateful clusters: PVC-backed volumes from nodeSpec.VolumeClaimTemplates.
//     The default data mount is appended by AppendDefaultDataVolumeMount.
//   - Stateless clusters: a single ephemeral volume (named elasticsearch-cache)
//     mounted at the ES data path. If the user supplied a VCT named
//     elasticsearch-cache, its spec is used; otherwise a small default is
//     injected.
//
// Other volumes (config, scripts, certs, users, file-settings, tmp, plugins,
// logs, downward-API) are identical between stateful and stateless.
func buildVolumes(
	es esv1.Elasticsearch,
	version version.Version,
	nodeSpec esv1.NodeSet,
	keystoreResources *keystore.Resources,
	downwardAPIVolume volume.DownwardAPI,
	additionalMountsFromPolicy []volume.VolumeLike,
	clientAuthenticationRequired bool,
) ([]corev1.Volume, []corev1.VolumeMount) {
	esName := es.Name
	// StatefulSet and Deployment share the same naming scheme for the pods
	// controller resource, so the transport/config volume names derive from the
	// same string in both modes.
	controllerName := esv1.StatefulSet(esName, nodeSpec.Name)

	configVolume := settings.ConfigSecretVolume(controllerName)

	// The pre-stop hook authenticates as the pre-stop user; stateless has no
	// pre-stop hook so we skip mounting that credential.
	probeUsers := []string{user.ProbeUserName}
	if !es.IsStateless() {
		probeUsers = append(probeUsers, user.PreStopUserName)
	}
	probeSecret := volume.NewSelectiveSecretVolumeWithMountPath(
		esv1.InternalUsersSecret(esName), esvolume.ProbeUserVolumeName,
		esvolume.PodMountedUsersSecretMountPath, probeUsers,
	)

	httpCertificatesVolume := volume.NewSecretVolumeWithMountPath(
		certificates.InternalCertsSecretName(esv1.ESNamer, esName),
		esvolume.HTTPCertificatesSecretVolumeName,
		esvolume.HTTPCertificatesSecretVolumeMountPath,
	)
	transportCertsVolume := transportCertificatesVolume(controllerName)
	remoteCertificateAuthoritiesVolume := volume.NewSecretVolumeWithMountPath(
		esv1.RemoteCaSecretName(esName),
		esvolume.RemoteCertificateAuthoritiesSecretVolumeName,
		esvolume.RemoteCertificateAuthoritiesSecretVolumeMountPath,
	)
	unicastHostsVolume := volume.NewConfigMapVolume(
		esv1.UnicastHostsConfigMap(esName), esvolume.UnicastHostsVolumeName, esvolume.UnicastHostsVolumeMountPath,
	)
	usersSecretVolume := volume.NewSecretVolumeWithMountPath(
		esv1.RolesAndFileRealmSecret(esName),
		esvolume.XPackFileRealmVolumeName,
		esvolume.XPackFileRealmVolumeMountPath,
	)
	scriptsVolume := volume.NewConfigMapVolumeWithMode(
		esv1.ScriptsConfigMap(esName),
		esvolume.ScriptsVolumeName,
		esvolume.ScriptsVolumeMountPath,
		0755)
	fileSettingsVolume := volume.NewSecretVolumeWithMountPath(
		esv1.FileSettingsSecretName(esName),
		esvolume.FileSettingsVolumeName,
		esvolume.FileSettingsVolumeMountPath,
	)
	tmpVolume := volume.NewEmptyDirVolume(
		esvolume.TempVolumeName,
		esvolume.TempVolumeMountPath,
	)

	// Data volume:
	//   - Stateful: placeholder PVC volumes for each VCT; actual claim name
	//     resolved at pod creation time. The default data mount is appended by
	//     AppendDefaultDataVolumeMount below.
	//   - Stateless: a single ephemeral cache volume mounted at the ES data
	//     path.
	var dataVolumes []corev1.Volume
	var dataVolumeMounts []corev1.VolumeMount
	if es.IsStateless() {
		dataVolumes = []corev1.Volume{defaultStatelessDataVolume(nodeSpec)}
		dataVolumeMounts = []corev1.VolumeMount{{
			Name:      esvolume.ElasticsearchCacheVolumeName,
			MountPath: esvolume.ElasticsearchDataMountPath,
		}}
	} else {
		dataVolumes = make([]corev1.Volume, 0, len(nodeSpec.VolumeClaimTemplates))
		for _, claimTemplate := range nodeSpec.VolumeClaimTemplates {
			dataVolumes = append(dataVolumes, corev1.Volume{
				Name: claimTemplate.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						// actual claim name will be resolved and fixed right before pod creation
						ClaimName: "claim-name-placeholder",
					},
				},
			})
		}
	}

	volumes := append(
		dataVolumes,
		append(
			initcontainer.PluginVolumes.Volumes(),
			esvolume.DefaultLogsVolume,
			usersSecretVolume.Volume(),
			unicastHostsVolume.Volume(),
			probeSecret.Volume(),
			transportCertsVolume.Volume(),
			remoteCertificateAuthoritiesVolume.Volume(),
			httpCertificatesVolume.Volume(),
			scriptsVolume.Volume(),
			configVolume.Volume(),
			downwardAPIVolume.Volume(),
			tmpVolume.Volume(),
		)...)
	if keystoreResources != nil {
		volumes = append(volumes, keystoreResources.Volume)
	}

	volumeMounts := append(
		initcontainer.PluginVolumes.ContainerVolumeMounts(),
		append(
			[]corev1.VolumeMount{
				esvolume.DefaultLogsVolumeMount,
				usersSecretVolume.VolumeMount(),
				unicastHostsVolume.VolumeMount(),
				probeSecret.VolumeMount(),
				transportCertsVolume.VolumeMount(),
				remoteCertificateAuthoritiesVolume.VolumeMount(),
				httpCertificatesVolume.VolumeMount(),
				scriptsVolume.VolumeMount(),
				configVolume.VolumeMount(),
				downwardAPIVolume.VolumeMount(),
				tmpVolume.VolumeMount(),
			},
			dataVolumeMounts...,
		)...,
	)

	// version gate for the file-based settings volume and volumeMounts
	if version.GTE(filesettings.FileBasedSettingsMinPreVersion) {
		volumes = append(volumes, fileSettingsVolume.Volume())
		volumeMounts = append(volumeMounts, fileSettingsVolume.VolumeMount())
	}

	// Mount the client trust bundle and the internal client certificate when client certificate validation is enabled.
	if clientAuthenticationRequired {
		trustBundleVolume := volume.NewSecretVolumeWithMountPath(
			certificates.ClientCertTrustBundleSecretName(esv1.ESNamer, esName),
			esvolume.ClientCertificatesTrustBundleVolumeName,
			esvolume.ClientCertificatesTrustBundleMountPath,
		)
		volumes = append(volumes, trustBundleVolume.Volume())
		volumeMounts = append(volumeMounts, trustBundleVolume.VolumeMount())

		// Mount the internal (operator) client certificate so the pre-stop hook can authenticate via mTLS.
		internalClientCertVolume := volume.NewSecretVolumeWithMountPath(
			certificates.OperatorClientCertSecretName(esv1.ESNamer, esName),
			esvolume.InternalClientCertVolumeName,
			esvolume.InternalClientCertMountPath,
		)
		volumes = append(volumes, internalClientCertVolume.Volume())
		volumeMounts = append(volumeMounts, internalClientCertVolume.VolumeMount())
	}

	// additional volumes from stack config policy
	for _, vol := range additionalMountsFromPolicy {
		volumes = append(volumes, vol.Volume())
		volumeMounts = append(volumeMounts, vol.VolumeMount())
	}

	if !es.IsStateless() {
		// Allow users to override the default data volume via podTemplate.
		volumeMounts = esvolume.AppendDefaultDataVolumeMount(volumeMounts, append(volumes, nodeSpec.PodTemplate.Spec.Volumes...))
	}

	return volumes, volumeMounts
}

// defaultStatelessDataVolume returns the ephemeral data ("cache") volume for a
// stateless Elasticsearch pod. If the user provided a VolumeClaimTemplate named
// elasticsearch-cache in the NodeSet, its spec is used; otherwise a small
// default is injected using the cluster's default storage class.
func defaultStatelessDataVolume(nodeSet esv1.NodeSet) corev1.Volume {
	spec := esvolume.DefaultDataVolumeClaim.Spec.DeepCopy()
	for _, vct := range nodeSet.VolumeClaimTemplates {
		if vct.Name == esvolume.ElasticsearchCacheVolumeName {
			spec = vct.Spec.DeepCopy()
			break
		}
	}
	return corev1.Volume{
		Name: esvolume.ElasticsearchCacheVolumeName,
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					Spec: *spec,
				},
			},
		},
	}
}
