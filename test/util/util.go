package util

import (
	"fmt"
	"math/rand"
	"sync"
)

var (
	usedResourceNames = sync.Map{}
)

func GetUniqueName(prefix string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, 8)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	name := fmt.Sprintf("%s%s", prefix, string(buf))
	_, loaded := usedResourceNames.LoadOrStore(name, true)
	if loaded {
		return GetUniqueName(prefix)
	}

	return name
}
