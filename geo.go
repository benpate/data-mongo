package mongodb

// GeoJSONer interface wraps the GeoJSON method, which generates
// a GeoJSON object for a given dataset
type GeoJSONer interface {

	// GeoJSON returns a GeoJSON representation of a geometric shape.
	GeoJSON() map[string]any
}
