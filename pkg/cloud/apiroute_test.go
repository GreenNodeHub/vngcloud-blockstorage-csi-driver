package cloud

import (
	ltesting "testing"
)

// The route label is the one place in this feature where getting it wrong is
// expensive rather than merely wrong: a raw ID leaking into a label mints a new
// time series per volume, per server and per snapshot.
func TestNormalizeAPIRouteCollapsesIdentifiers(t *ltesting.T) {
	// The real endpoint carries its own path segments; they must not appear in
	// the label. This is the shape the SDK builds (see identity_test.go upstream).
	const base = "https://hcm-3.api.vngcloud.vn/vserver/vserver-gateway/v2/"

	for _, tc := range []struct {
		name string
		url  string
		want string
	}{
		{
			name: "create volume",
			url:  base + "proj-8ff1/volumes",
			want: "volumes",
		},
		{
			name: "get one volume",
			url:  base + "proj-8ff1/volumes/vol-4c2b1e0a-6f1d-4a2b",
			want: "volumes/{id}",
		},
		{
			name: "attach is a five-segment path with two ids",
			url:  base + "proj-8ff1/volumes/vol-4c2b/servers/ins-9a7f/attach",
			want: "volumes/{id}/servers/{id}/attach",
		},
		{
			name: "detach",
			url:  base + "proj-8ff1/volumes/vol-4c2b/servers/ins-9a7f/detach",
			want: "volumes/{id}/servers/{id}/detach",
		},
		{
			name: "list query is dropped, page numbers must not become labels",
			url:  base + "proj-8ff1/volumes?page=3&size=100&name=pvc-abc",
			want: "volumes",
		},
		{
			name: "snapshot of a volume",
			url:  base + "proj-8ff1/volumes/vol-4c2b/snapshots/snap-77",
			want: "volumes/{id}/snapshots/{id}",
		},
		{
			name: "resize",
			url:  base + "proj-8ff1/volumes/vol-4c2b/resize",
			want: "volumes/{id}/resize",
		},
		{
			name: "migrate keeps its hyphenated segment",
			url:  base + "proj-8ff1/volumes/vol-4c2b/change-device-type",
			want: "volumes/{id}/change-device-type",
		},
		{
			// The case that rules out any positional scheme: the SDK puts a
			// zone ID BEFORE the static segment here.
			name: "volume types has an id before the static segment",
			url:  base + "proj-8ff1/zone-hcm3a/volume_types",
			want: "volume_types",
		},
		{
			name: "portal info",
			url:  "https://iam.vngcloud.vn/portal/v1/projects/p-991/detail",
			want: "projects/{id}/detail",
		},
		{
			name: "trailing slash does not add a segment",
			url:  base + "proj-8ff1/volumes/",
			want: "volumes",
		},
		{
			name: "doubled slash does not add a segment",
			url:  base + "proj-8ff1//volumes",
			want: "volumes",
		},
		{
			name: "a path-only url still normalises",
			url:  "/proj-8ff1/volumes/vol-4c2b",
			want: "volumes/{id}",
		},
		{
			name: "no path at all",
			url:  "https://hcm-3.api.vngcloud.vn",
			want: routeUnknown,
		},
		{
			name: "empty url",
			url:  "",
			want: routeUnknown,
		},
		{
			name: "a url with no recognised segment is not labelled",
			url:  base + "a/b/c/d/e/f/g/h",
			want: routeUnknown,
		},
		{
			// The length bound is the second guard: once a route has started,
			// it stops an unexpectedly deep path from minting a new shape.
			name: "a recognised route followed by too many segments",
			url:  base + "proj-8ff1/volumes/a/b/c/d/e/f",
			want: routeUnknown,
		},
		{
			name: "a fragment is dropped like a query",
			url:  base + "proj-8ff1/volumes#anchor",
			want: "volumes",
		},
	} {
		t.Run(tc.name, func(t *ltesting.T) {
			if got := NormalizeAPIRoute(tc.url); got != tc.want {
				t.Errorf("NormalizeAPIRoute(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}

// Two volumes must share one series. This is the property the whole normaliser
// exists for, asserted directly rather than inferred from the table above.
func TestNormalizeAPIRouteIsIdenticalForTwoDifferentVolumes(t *ltesting.T) {
	const base = "https://vserver/vserver-gateway/v2/proj-1/volumes/"

	a := NormalizeAPIRoute(base + "vol-aaaaaaaa-1111-2222-3333-444444444444")
	b := NormalizeAPIRoute(base + "vol-bbbbbbbb-5555-6666-7777-888888888888")

	if a != b {
		t.Fatalf("two volumes produced different routes: %q vs %q", a, b)
	}
	if a != "volumes/{id}" {
		t.Fatalf("route = %q, want volumes/{id}", a)
	}
}

// KnownAPIRoutes only earns its place if its strings are the ones the
// normaliser actually emits. A typo there would pre-create a series that never
// fills while the real one registers lazily beside it - two series, one of them
// permanently zero, and no error anywhere.
func TestKnownAPIRoutesAreFixedPointsOfTheNormaliser(t *ltesting.T) {
	for _, rm := range KnownAPIRoutes() {
		// Build the URL the way the SDK does: endpoint, then the route's
		// segments. Reversing {id} back into a plausible identifier is what
		// makes this a round trip rather than a tautology.
		url := "https://vserver/vserver-gateway/v2/proj-1/"
		for i, seg := range splitRoute(rm.Route) {
			if i > 0 {
				url += "/"
			}
			if seg == routeIDPlaceholder {
				url += "abc-1234"
				continue
			}
			url += seg
		}

		if got := NormalizeAPIRoute(url); got != rm.Route {
			t.Errorf("route %q (%s) round-tripped to %q", rm.Route, rm.Method, got)
		}
	}
}

func TestKnownAPIRoutesHasNoDuplicatePairs(t *ltesting.T) {
	seen := make(map[RouteMethod]struct{})
	for _, rm := range KnownAPIRoutes() {
		if _, dup := seen[rm]; dup {
			t.Errorf("duplicate (route, method) pair: %q %s", rm.Route, rm.Method)
		}
		seen[rm] = struct{}{}

		if rm.Method == "" || rm.Route == "" {
			t.Errorf("empty field in pair %+v", rm)
		}
	}
}

func splitRoute(proute string) []string {
	out := make([]string, 0, maxRouteSegments)
	start := 0
	for i := 0; i <= len(proute); i++ {
		if i == len(proute) || proute[i] == '/' {
			if i > start {
				out = append(out, proute[start:i])
			}
			start = i + 1
		}
	}

	return out
}
