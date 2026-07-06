package codec

import (
	"bytes"
	"reflect"
	"testing"
)

func TestJsonCodec_Encode(t *testing.T) {
	type User struct {
		Name string `json:"name"`
	}
	type testCase struct {
		name          string
		jc            *JsonCodec[User]
		val           any
		want          []byte
		wantErrEncode bool
	}
	tests := []testCase{
		{
			name: "encode_user_struct",
			jc:   NewJsonCodec[User](),
			val: User{
				Name: "John",
			},
			want:          []byte(`{"name":"John"}`),
			wantErrEncode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.jc.Encode(tt.val)
			if (err != nil) != tt.wantErrEncode {
				t.Errorf("Encode() error = %v, wantErrEncode %v", err, tt.wantErrEncode)
			}
			if bytes.Compare(b, tt.want) != 0 {
				t.Errorf("Encode() got = %v, want %v", b, tt.want)
			}
		})
	}
}

func TestJsonCodec_Decode(t *testing.T) {
	type User struct {
		Name string `json:"name"`
	}
	type testCase struct {
		name          string
		jc            *JsonCodec[User]
		val           any
		want          any
		wantErrDecode bool
	}
	tests := []testCase{
		{
			name: "decode_user_struct",
			jc:   NewJsonCodec[User](),
			val: User{
				Name: "John",
			},
			want:          User{Name: "John"},
			wantErrDecode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := tt.jc.Encode(tt.want)
			if err != nil {
				t.Errorf("Encode() error = %v", err)
			}
			decode, err := tt.jc.Decode(b)
			if (err != nil) != tt.wantErrDecode {
				t.Errorf("Decode() error = %v, wantErrDecode %v", err, tt.wantErrDecode)
			}
			if !reflect.DeepEqual(decode, tt.want) {
				t.Errorf("Decode() got = %v, want %v", decode, tt.want)
			}
		})
	}
}
