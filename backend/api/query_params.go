package api

import (
	"strconv"
	"strings"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

func ParseLimitOffset(limitRaw, offsetRaw string) (limit int, offset int, err error) {
	limit = DefaultLimit
	offset = 0

	if strings.TrimSpace(limitRaw) != "" {
		v, e := strconv.Atoi(limitRaw)
		if e != nil {
			return 0, 0, e
		}
		limit = v
	}
	if strings.TrimSpace(offsetRaw) != "" {
		v, e := strconv.Atoi(offsetRaw)
		if e != nil {
			return 0, 0, e
		}
		offset = v
	}

	if limit < 1 {
		limit = 1
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}
	if offset < 0 {
		offset = 0
	}

	return limit, offset, nil
}
