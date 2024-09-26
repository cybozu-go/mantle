package util

import (
	"fmt"
	"math/rand"
)

var (
	usedResourceNames = make(map[string]bool)
)

func GetUniqueName(prefix string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, 8)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	name := fmt.Sprintf("%s%s", prefix, string(buf))
	if usedResourceNames[name] {
		return GetUniqueName(prefix)
	}
	usedResourceNames[name] = true
	return name
}
