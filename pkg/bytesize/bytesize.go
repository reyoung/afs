package bytesize

import (
	"fmt"
	"strconv"
	"strings"
)

var binaryUnits = map[string]int64{
	"":    1,
	"b":   1,
	"k":   1 << 10,
	"kb":  1 << 10,
	"kib": 1 << 10,
	"m":   1 << 20,
	"mb":  1 << 20,
	"mib": 1 << 20,
	"g":   1 << 30,
	"gb":  1 << 30,
	"gib": 1 << 30,
}

func Parse(value string) (int64, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return 0, fmt.Errorf("size is required")
	}

	split := len(raw)
	for i, r := range raw {
		if r < '0' || r > '9' {
			split = i
			break
		}
	}
	if split == 0 {
		return 0, fmt.Errorf("size %q must start with digits", value)
	}

	number, err := strconv.ParseInt(raw[:split], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", value, err)
	}
	if number <= 0 {
		return 0, fmt.Errorf("size %q must be > 0", value)
	}

	unit := strings.ToLower(strings.TrimSpace(raw[split:]))
	multiplier, ok := binaryUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unsupported size suffix %q", raw[split:])
	}
	if number > (1<<63-1)/multiplier {
		return 0, fmt.Errorf("size %q overflows int64", value)
	}
	return number * multiplier, nil
}
