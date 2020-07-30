package kibana

import (
	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/keystore"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// keystoreInConfigDirVersion is the version in which the keystore is no longer stored in the data directory but in the configuration one.
var keystoreInConfigDirVersion = version.From(7, 9, 0)

// newInitContainersParameters is used to generate the init container that will load the secure settings into a keystore
func newInitContainersParameters(kb *kbv1.Kibana) (keystore.InitContainerParameters, error) {
	parameters := keystore.InitContainerParameters{
		KeystoreCreateCommand:         "/usr/share/kibana/bin/kibana-keystore create",
		KeystoreAddCommand:            `/usr/share/kibana/bin/kibana-keystore add "$key" --stdin < "$filename"`,
		SecureSettingsVolumeMountPath: keystore.SecureSettingsVolumeMountPath,
		KeystoreVolumePath:            DataVolumeMountPath,
		Resources: corev1.ResourceRequirements{
			Requests: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceMemory: resource.MustParse("128Mi"),
				corev1.ResourceCPU:    resource.MustParse("100m"),
			},
			Limits: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceMemory: resource.MustParse("128Mi"),
				corev1.ResourceCPU:    resource.MustParse("100m"),
			},
		},
	}

	kbVersion, err := version.Parse(kb.Spec.Version)
	if err != nil {
		return parameters, err
	}

	if kbVersion.IsSameOrAfter(keystoreInConfigDirVersion) {
		parameters.KeystoreVolumePath = ConfigSharedVolume.ContainerMountPath
	}

	return parameters, nil
}
