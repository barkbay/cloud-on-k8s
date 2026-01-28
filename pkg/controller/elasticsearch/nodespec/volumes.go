// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package nodespec

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
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

func buildVolumes(
	cluster escommon.ElasticsearchCluster,
	isStateless bool,
	ver version.Version,
	nodeSet escommon.NodeSetSpec,
	keystoreResources *keystore.Resources,
	downwardAPIVolume volume.DownwardAPI,
	additionalMountsFromPolicy []volume.VolumeLike,
) ([]corev1.Volume, []corev1.VolumeMount) {
	esName := cluster.GetName()
	configVolume := settings.ConfigSecretVolume(escommon.PodControllerName(cluster, nodeSet.GetName()), cluster.IsStateless())
	probeSecret := volume.NewSelectiveSecretVolumeWithMountPath(
		escommon.InternalUsersSecret(cluster), esvolume.ProbeUserVolumeName,
		esvolume.PodMountedUsersSecretMountPath, []string{user.ProbeUserName, user.PreStopUserName},
	)
	httpCertificatesVolume := volume.NewSecretVolumeWithMountPath(
		certificates.InternalCertsSecretName(escommon.NamerFor(cluster), esName),
		esvolume.HTTPCertificatesSecretVolumeName,
		esvolume.HTTPCertificatesSecretVolumeMountPath,
	)
	transportCertificatesVolume := transportCertificatesVolume(escommon.PodControllerName(cluster, nodeSet.GetName()), cluster.IsStateless())
	remoteCertificateAuthoritiesVolume := volume.NewSecretVolumeWithMountPath(
		escommon.RemoteCaSecretName(cluster),
		esvolume.RemoteCertificateAuthoritiesSecretVolumeName,
		esvolume.RemoteCertificateAuthoritiesSecretVolumeMountPath,
	)
	unicastHostsVolume := volume.NewConfigMapVolume(
		escommon.UnicastHostsConfigMap(cluster), esvolume.UnicastHostsVolumeName, esvolume.UnicastHostsVolumeMountPath,
	)
	usersSecretVolume := volume.NewSecretVolumeWithMountPath(
		escommon.RolesAndFileRealmSecret(cluster),
		esvolume.XPackFileRealmVolumeName,
		esvolume.XPackFileRealmVolumeMountPath,
	)
	scriptsVolume := volume.NewConfigMapVolumeWithMode(
		escommon.ScriptsConfigMap(cluster),
		esvolume.ScriptsVolumeName,
		esvolume.ScriptsVolumeMountPath,
		0755)
	fileSettingsVolume := volume.NewSecretVolumeWithMountPath(
		escommon.FileSettingsSecretName(cluster),
		esvolume.FileSettingsVolumeName,
		esvolume.FileSettingsVolumeMountPath,
	)
	tmpVolume := volume.NewEmptyDirVolume(
		esvolume.TempVolumeName,
		esvolume.TempVolumeMountPath,
	)
	
	volumes := claimTemplatesToVolumes(nodeSet.GetVolumeClaimTemplates(), isStateless)

	volumes = append(
		volumes, // includes the data volume, unless specified differently in the pod template
		append(
			initcontainer.PluginVolumes.Volumes(),
			esvolume.DefaultLogsVolume,
			usersSecretVolume.Volume(),
			unicastHostsVolume.Volume(),
			probeSecret.Volume(),
			transportCertificatesVolume.Volume(),
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
		esvolume.DefaultLogsVolumeMount,
		usersSecretVolume.VolumeMount(),
		unicastHostsVolume.VolumeMount(),
		probeSecret.VolumeMount(),
		transportCertificatesVolume.VolumeMount(),
		remoteCertificateAuthoritiesVolume.VolumeMount(),
		httpCertificatesVolume.VolumeMount(),
		scriptsVolume.VolumeMount(),
		configVolume.VolumeMount(),
		downwardAPIVolume.VolumeMount(),
		tmpVolume.VolumeMount(),
	)

	// version gate for the file-based settings volume and volumeMounts
	if isStateless || ver.GTE(filesettings.FileBasedSettingsMinPreVersion) {
		volumes = append(volumes, fileSettingsVolume.Volume())
		volumeMounts = append(volumeMounts, fileSettingsVolume.VolumeMount())
	}

	// additional volumes from stack config policy
	for _, volume := range additionalMountsFromPolicy {
		volumes = append(volumes, volume.Volume())
		volumeMounts = append(volumeMounts, volume.VolumeMount())
	}

	// include the user-provided PodTemplate volumes as the user may have defined the data volume there (e.g.: emptyDir or hostpath volume)
	volumeMounts = esvolume.AppendDefaultDataVolumeMount(volumeMounts, append(volumes, nodeSet.GetPodTemplate().Spec.Volumes...))

	return volumes, volumeMounts
}

// claimTemplatesToVolumes converts PersistentVolumeClaim templates to Volumes.
// For stateless mode, it creates ephemeral volumes. For stateful mode, it creates
// PVC-backed volumes with placeholder claim names (resolved before pod creation).
func claimTemplatesToVolumes(claimTemplates []corev1.PersistentVolumeClaim, isStateless bool) []corev1.Volume {
	volumes := make([]corev1.Volume, 0, len(claimTemplates))
	for _, claim := range claimTemplates {
		volumes = append(volumes, corev1.Volume{
			Name:         claim.Name,
			VolumeSource: claimToVolumeSource(claim, isStateless),
		})
	}
	return volumes
}

// claimToVolumeSource returns the appropriate VolumeSource for a claim template.
func claimToVolumeSource(claim corev1.PersistentVolumeClaim, isStateless bool) corev1.VolumeSource {
	if isStateless {
		return corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: claim.ObjectMeta.Annotations,
						Labels:      claim.ObjectMeta.Labels,
					},
					Spec: claim.Spec,
				},
			},
		}
	}
	return corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			// actual claim name will be resolved and fixed right before pod creation
			ClaimName: "claim-name-placeholder",
		},
	}
}
