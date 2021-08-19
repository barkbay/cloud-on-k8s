# Elastic Cloud on Kubernetes 1.6.0

- [Release highlights](https://www.elastic.co/guide/en/cloud-on-k8s/1.7/release-highlights-1.7.0.html)
- [Quickstart guide](https://www.elastic.co/guide/en/cloud-on-k8s/1.7/k8s-quickstart.html)

##  Breaking changes

* Split manifest generation to produce both v1/v1beta1 CRDs #4489

## New features

* [Stack Monitoring] Kibana monitoring with Metricbeat and Filebeat as sidecars #4183
* [Stack Monitoring] Elasticsearch monitoring with Metricbeat and Filebeat as sidecars #4528
* [Agent] Fleet mode and Fleet Server support in Elastic Agent CRD #4429
* [Autoscaling] Enable scale subresource on eligible resources #4547
* Simplify setup of Enterprise Search access through Kibana UI #4598
* Support custom CA on HTTP layer #4522

##  Enhancements

* Add support for Elliptic Curve Digital Signature Algorithm (ECDSA) signed certificates #4581
* Allow users to configure SANs on transport certificates #4600
* Add validation for declared volume claim templates #4526]
* [Autoscaling] Introduce resource recommenders #4493

##  Bug fixes

* Beats: allow unprivileged Pods to read configuration file #4562
* Fix heap memory parsing with trailing white spaces #4564
* Detect admission registration API version #4555