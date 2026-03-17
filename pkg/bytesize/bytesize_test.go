package bytesize

import "testing"

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{input: "8M", want: 8 << 20},
		{input: "16MiB", want: 16 << 20},
		{input: "32mb", want: 32 << 20},
		{input: "4096", want: 4096},
		{input: "0", wantErr: true},
		{input: "bad", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) error = nil, want error", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("Parse(%q) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}
