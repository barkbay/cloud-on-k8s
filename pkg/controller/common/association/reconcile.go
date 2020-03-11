package association

type AssociationReconciler interface {
	Name() string
	AssociationLabelName() string
	AssociationLabelNamespace() string

	BackendUserSuffix(kind string) string
	BackendCaSecret(kind string) string
}
