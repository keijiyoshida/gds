package highcharts

// Chart represents a chart.
type Chart struct {
	Type     string
	Title    string
	Subtitle string
	XAxis    string
	YAxis    string
	Series   []Series
}
