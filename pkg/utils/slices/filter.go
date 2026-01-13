// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package slices

// Filter filters the items that match the predicate f.
func Filter[T any](slice []T, f func(T) bool) []T {
	if slice == nil {
		return nil
	}
	result := make([]T, 0, len(slice))
	for _, x := range slice {
		if f(x) {
			result = append(result, x)
		}
	}
	return result
}
