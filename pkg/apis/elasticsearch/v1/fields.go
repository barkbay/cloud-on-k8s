// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

const (
	ClusterName = "cluster.name"

	DiscoveryZenMinimumMasterNodes = "discovery.zen.minimum_master_nodes"
	ClusterInitialMasterNodes      = "cluster.initial_master_nodes"

	DiscoveryZenHostsProvider = "discovery.zen.hosts_provider" // ES < 7.X
	DiscoverySeedProviders    = "discovery.seed_providers"     // ES >= 7.X

	// XPackMLUseAutoMachineMemoryPercent lets ML calculate machine’s memory that can be used
	// for running machine learning analytics processes automatically.
	// It is set to true automatically for ML nodes when autoscaling is enabled.
	XPackMLUseAutoMachineMemoryPercent = "xpack.ml.use_auto_machine_memory_percent"

	// WatermarkEnableForSingleDataNode is automatically enabled with autoscaling to allow single node data cluster to autoscale.
	WatermarkEnableForSingleDataNode = "cluster.routing.allocation.disk.watermark.enable_for_single_data_node"

	NetworkHost        = "network.host"
	NetworkPublishHost = "network.publish_host"
	HTTPPublishHost    = "http.publish_host"

	NodeName = "node.name"

	PathData = "path.data"
	PathLogs = "path.logs"

	ShardAwarenessAttributes = "cluster.routing.allocation.awareness.attributes"
	NodeAttr                 = "node.attr"

	XPackSecurityAuthcRealmsFileFile1Order     = "xpack.security.authc.realms.file.file1.order"     // 7.x realm syntax
	XPackSecurityAuthcRealmsFile1Order         = "xpack.security.authc.realms.file1.order"          // 6.x realm syntax
	XPackSecurityAuthcRealmsFile1Type          = "xpack.security.authc.realms.file1.type"           // 6.x realm syntax
	XPackSecurityAuthcRealmsNativeNative1Order = "xpack.security.authc.realms.native.native1.order" // 7.x realm syntax
	XPackSecurityAuthcRealmsNative1Order       = "xpack.security.authc.realms.native1.order"        // 6.x realm syntax
	XPackSecurityAuthcRealmsNative1Type        = "xpack.security.authc.realms.native1.type"         // 6.x realm syntax

	XPackSecurityAuthcReservedRealmEnabled          = "xpack.security.authc.reserved_realm.enabled"
	XPackSecurityEnabled                            = "xpack.security.enabled"
	XPackSecurityHttpSslCertificate                 = "xpack.security.http.ssl.certificate"
	XPackSecurityHttpSslCertificateAuthorities      = "xpack.security.http.ssl.certificate_authorities"
	XPackSecurityHttpSslEnabled                     = "xpack.security.http.ssl.enabled"
	XPackSecurityHttpSslKey                         = "xpack.security.http.ssl.key"
	XPackSecurityTransportSslCertificate            = "xpack.security.transport.ssl.certificate"
	XPackSecurityTransportSslCertificateAuthorities = "xpack.security.transport.ssl.certificate_authorities"
	XPackSecurityTransportSslEnabled                = "xpack.security.transport.ssl.enabled"
	XPackSecurityTransportSslKey                    = "xpack.security.transport.ssl.key"
	XPackSecurityTransportSslVerificationMode       = "xpack.security.transport.ssl.verification_mode"

	XPackLicenseUploadTypes = "xpack.license.upload.types" // supported >= 7.6.0 used as of 7.8.1
)

var UnsupportedSettings = []string{
	ClusterName,
	DiscoveryZenMinimumMasterNodes,
	ClusterInitialMasterNodes,
	NetworkHost,
	NetworkPublishHost,
	NodeName,
	PathData,
	PathLogs,
	XPackSecurityAuthcReservedRealmEnabled,
	XPackSecurityEnabled,
	XPackSecurityHttpSslCertificate,
	XPackSecurityHttpSslEnabled,
	XPackSecurityHttpSslKey,
	XPackSecurityTransportSslCertificate,
	XPackSecurityTransportSslEnabled,
	XPackSecurityTransportSslKey,
	XPackSecurityTransportSslVerificationMode,
}
