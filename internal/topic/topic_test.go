package topic

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    Info
		wantErr bool
	}{
		{
			name: "persistent full",
			raw:  "persistent://tenant/ns/topic",
			want: Info{
				Persistent: true,
				Tenant:     "tenant",
				Namespace:  "ns",
				Name:       "topic",
				FullName:   "persistent://tenant/ns/topic",
			},
		},
		{
			name: "non-persistent full",
			raw:  "non-persistent://tenant/ns/topic",
			want: Info{
				Persistent: false,
				Tenant:     "tenant",
				Namespace:  "ns",
				Name:       "topic",
				FullName:   "non-persistent://tenant/ns/topic",
			},
		},
		{
			name: "defaults to persistent",
			raw:  "orders",
			want: Info{
				Persistent: true,
				Tenant:     "public",
				Namespace:  "default",
				Name:       "orders",
				FullName:   "persistent://public/default/orders",
			},
		},
		{
			name:    "empty",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			raw:     "http://tenant/ns/topic",
			wantErr: true,
		},
		{
			name:    "missing parts",
			raw:     "persistent://tenant/ns",
			wantErr: true,
		},
		{
			name:    "missing name",
			raw:     "persistent://tenant/ns/",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected info: %#v", got)
			}
		})
	}
}
