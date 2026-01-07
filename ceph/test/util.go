package test

import (
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"
)

func Quantity2Str(qStr string) string {
	return strconv.FormatUint(Quantity2Int(qStr), 10)
}

func Quantity2Int(qStr string) uint64 {
	q := resource.MustParse(qStr)

	return uint64(q.Value())
}
