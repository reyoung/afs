//go:build !linux

package afslet

func StartChildReaper(func(string, ...any)) func() {
	return func() {}
}

func HoldReaper() func() {
	return func() {}
}
