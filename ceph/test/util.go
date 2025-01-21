package test

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

func Quantity2Str(qStr string) string {
	return fmt.Sprintf("%d", Quantity2Int(qStr))
}

func Quantity2Int(qStr string) uint64 {
	q := resource.MustParse(qStr)
	return uint64(q.Value())
}
