package cloud

import (
	lstr "strings"
)

// Turning a vServer URL into a metric label.
//
// The label has to identify WHICH API was called while staying bounded in
// cardinality, and the raw URL does neither: it carries project, volume,
// server and snapshot IDs.
//
// aws-ebs-csi-driver does not have this problem - the AWS SDK's middleware
// carries an operation name (GetOperationName), so its metrics label by
// operation directly. The vngcloud SDK's IRequest exposes only
// GetRequestMethod(); there is no operation name at the transport layer, which
// is the only layer every call passes through. So the route is derived from
// the URL.
//
// The derivation keeps a segment if and only if it is in a whitelist of
// segments that describe an API's SHAPE, and replaces every other segment with
// "{id}".
//
// Why a whitelist rather than the two obvious alternatives:
//
//   - A regex that recognises IDs (UUID, "vol-" prefix, ...) fails the day
//     vServer changes an ID format, and it fails SILENTLY into unbounded
//     cardinality - the worst possible failure for a metric label.
//   - Mapping by position fails on a real URL already in the SDK:
//     getVolumeTypesUrl builds "<project>/<zoneId>/volume_types", putting an ID
//     BEFORE the static segment. Nothing positional survives that.
//
// A whitelist can only be wrong when the SDK adds a new static segment, and
// then it is wrong in the safe direction: the new segment reads as "{id}",
// cardinality stays bounded, and the coarser route is visible on any dashboard.
var staticRouteSegments = map[string]struct{}{
	// volume/v2
	"volumes":            {},
	"servers":            {},
	"snapshots":          {},
	"resize":             {},
	"mapping":            {},
	"change-device-type": {},
	"tag":                {},
	"resource":           {},
	// compute/v2
	"attach": {},
	"detach": {},
	// volume/v1
	"volume_types":      {},
	"volume_default_id": {},
	"volume_type_zones": {},
	// portal/v1 + portal/v2
	"projects":  {},
	"detail":    {},
	"zones":     {},
	"quotas":    {},
	"quotaUsed": {},
}

// routeIDPlaceholder is what every non-whitelisted segment collapses to.
const routeIDPlaceholder = "{id}"

// routeUnknown is the label for a URL that contains no recognised segment at
// all. It exists so an unexpected URL shape cannot mint a new series.
const routeUnknown = "unknown"

// maxRouteSegments is one more than the longest route the SDK builds
// ("volumes/{id}/servers/{id}/attach" is 5), leaving room for one added
// segment before a URL is treated as unrecognised.
const maxRouteSegments = 6

// NormalizeAPIRoute derives a bounded-cardinality metric label from a request
// URL. It never returns an empty string, so a metric label is always valid.
//
// The route starts at the FIRST whitelisted segment and everything before it is
// discarded. That prefix is the endpoint's own path plus the project ID -
// vServer endpoints look like
// "https://hcm-3.api.vngcloud.vn/vserver/vserver-gateway", the service client
// appends a version, and every ServiceURL then starts with GetProjectId().
// Keeping any of that would put three or four placeholder segments in front of
// every route, which reads as noise, differs per region, and pushed the longest
// real route (attach) past the length bound entirely.
func NormalizeAPIRoute(purl string) string {
	path := purl

	// Strip scheme://host. Deliberately not net/url.Parse: this runs on every
	// single API call, and the only thing needed is the path.
	if i := lstr.Index(path, "://"); i >= 0 {
		rest := path[i+3:]
		if j := lstr.Index(rest, "/"); j >= 0 {
			path = rest[j:]
		} else {
			path = "/"
		}
	}

	// Drop the query and any fragment. List URLs carry a query built by
	// ToQuery(), which contains page numbers and names.
	if i := lstr.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}

	segments := make([]string, 0, maxRouteSegments)
	started := false

	for _, seg := range lstr.Split(path, "/") {
		if seg == "" {
			continue // leading, trailing and doubled slashes
		}

		_, static := staticRouteSegments[seg]

		if !started {
			if !static {
				continue // still in the endpoint prefix / project ID
			}
			started = true
		}

		if len(segments) >= maxRouteSegments {
			return routeUnknown
		}

		if static {
			segments = append(segments, seg)
		} else {
			segments = append(segments, routeIDPlaceholder)
		}
	}

	if len(segments) == 0 {
		return routeUnknown
	}

	return lstr.Join(segments, "/")
}

// RouteMethod is one (route, method) pair, which is the shape of a series in
// the api_request_* family.
type RouteMethod struct {
	Route  string
	Method string
}

// KnownAPIRoutes is every (route, method) pair this driver calls, so those
// series can exist at zero before the first call rather than appearing only
// once something happens. See InitializeStartupMetrics for why that matters.
//
// This list is documentation as much as data: it is the answer to "what does
// this driver ask vServer for". It is NOT load-bearing for correctness - a
// pair missing from here is still recorded, just registered lazily.
//
// Every method below was read off the SDK service call, not inferred from the
// verb an HTTP API "should" use: attach and detach are PUT, and resize and
// change-device-type are PUT rather than POST.
func KnownAPIRoutes() []RouteMethod {
	return []RouteMethod{
		{"volumes", "POST"},                         // CreateBlockVolume
		{"volumes", "GET"},                          // ListBlockVolumes
		{"volumes/{id}", "GET"},                     // GetBlockVolumeById
		{"volumes/{id}", "DELETE"},                  // DeleteBlockVolumeById
		{"volumes/{id}/resize", "PUT"},              // ResizeBlockVolumeById
		{"volumes/{id}/snapshots", "POST"},          // CreateSnapshotByBlockVolumeId
		{"volumes/{id}/snapshots", "GET"},           // ListSnapshotsByBlockVolumeId
		{"volumes/{id}/snapshots/{id}", "DELETE"},   // DeleteSnapshotById
		{"volumes/{id}/mapping", "GET"},             // GetUnderBlockVolumeId
		{"volumes/{id}/change-device-type", "PUT"},  // MigrateBlockVolumeById
		{"volumes/{id}/servers/{id}/attach", "PUT"}, // AttachBlockVolume
		{"volumes/{id}/servers/{id}/detach", "PUT"}, // DetachBlockVolume
		{"servers/{id}", "GET"},                     // GetServerById
		{"volume_types/{id}", "GET"},                // GetVolumeTypeById
		{"volume_default_id", "GET"},                // GetDefaultVolumeType
		{"volume_type_zones", "GET"},                // GetVolumeTypeZones
		{"volume_types", "GET"},                     // GetListVolumeTypes
		{"zones", "GET"},                            // ListZones
		{"projects/{id}/detail", "GET"},             // GetPortalInfo
	}
}
