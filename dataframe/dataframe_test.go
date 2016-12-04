package dataframe

import (
	"reflect"
	"testing"
)

func Test_getSrcItemNames(t *testing.T) {
	testCases := []struct {
		data             [][]string
		config           Config
		wantSrcItemNames []string
		wantErr          error
	}{
		{
			[][]string{},
			Config{nil, nil, true},
			nil,
			ErrNoData,
		},
		{
			[][]string{{"0"}},
			Config{nil, nil, true},
			[]string{"0"},
			nil,
		},
		{
			[][]string{},
			Config{[]string{"0"}, nil, false},
			[]string{"0"},
			nil,
		},
	}

	for _, tc := range testCases {
		gotSrcItemNames, gotErr := getSrcItemNames(tc.data, tc.config)

		if !reflect.DeepEqual(gotSrcItemNames, tc.wantSrcItemNames) || gotErr != tc.wantErr {
			t.Errorf("getSrcItemNames(%v, %v) => %v, %#v; want %v, %#v",
				tc.data, tc.config, gotSrcItemNames, gotErr, tc.wantSrcItemNames, tc.wantErr)
		}
	}
}

func Test_newItemNames(t *testing.T) {
	testCases := []struct {
		data          [][]string
		config        Config
		wantItemNames []string
		wantErr       error
	}{
		{
			[][]string{},
			Config{nil, nil, true},
			nil,
			ErrNoData,
		},
		{
			[][]string{{"0"}},
			Config{nil, nil, true},
			[]string{"0"},
			nil,
		},
	}

	for _, tc := range testCases {
		gotItemNames, gotErr := newItemNames(tc.data, tc.config)

		if !reflect.DeepEqual(gotItemNames, tc.wantItemNames) || gotErr != tc.wantErr {
			t.Errorf("newItemNames(%v, %v) => %v, %#v; want %v, %#v",
				tc.data, tc.config, gotItemNames, gotErr, tc.wantItemNames, tc.wantErr)
		}
	}
}

func Test_newTypes(t *testing.T) {
	testCases := []struct {
		itemNames []string
		srcTypes  []Type
		wantTypes map[string]Type
		wantErr   error
	}{
		{
			[]string{"0"},
			[]Type{String, Float64},
			nil,
			ErrInvalidTypesLen,
		},
		{
			[]string{"0"},
			[]Type{Type(-1)},
			nil,
			ErrInvalidType,
		},
		{
			[]string{"0", "0"},
			[]Type{String, Float64},
			nil,
			ErrDuplicatedItemName,
		},
		{
			[]string{"0", "1", "2", "3"},
			[]Type{String, Float64, String, Float64},
			map[string]Type{"0": String, "1": Float64, "2": String, "3": Float64},
			nil,
		},
	}

	for _, tc := range testCases {
		gotTypes, gotErr := newTypes(tc.itemNames, tc.srcTypes)

		if !reflect.DeepEqual(gotTypes, tc.wantTypes) || gotErr != tc.wantErr {
			t.Errorf("newTypes(%v, %v) => %v, %#v; want %v, %#v",
				tc.itemNames, tc.srcTypes, gotTypes, gotErr, tc.wantTypes, tc.wantErr)
		}
	}
}
