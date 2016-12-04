package dataframe

import "testing"

func TestType_valid(t *testing.T) {
	testCases := []struct {
		t    Type
		want bool
	}{
		{String, true},
		{Float64, true},
		{Type(-1), false},
	}

	for _, tc := range testCases {
		if got := tc.t.valid(); got != tc.want {
			t.Errorf("t.valid() => %t; want %t; t: %d", got, tc.want, tc.t)
		}
	}
}
