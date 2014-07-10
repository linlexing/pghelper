package pghelper

import (
	"strings"
)

type PGUrl map[string]string

func (u PGUrl) Parse(dburl string) {
	for k, _ := range u {
		delete(u, k)
	}
	for _, v := range strings.Split(dburl, " ") {
		vs := strings.Split(v, "=")
		if len(vs) == 2 {
			u[vs[0]] = vs[1]
		}
	}
}
func (u PGUrl) String() string {
	rev := []string{}
	for k, v := range u {
		rev = append(rev, k+"="+v)
	}
	return strings.Join(rev, " ")
}
func NewPGUrl(dburl string) PGUrl {
	rev := PGUrl{}
	rev.Parse(dburl)
	return rev
}
