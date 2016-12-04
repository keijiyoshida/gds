package highcharts

import (
	"html/template"
	"io"
	"io/ioutil"
	"os"
)

const tplStr = `
<html>
	<head>
		<script src="https://code.jquery.com/jquery-3.1.1.min.js"></script>
		<script src="http://code.highcharts.com/highcharts.js"></script>
	</head>
	<body>
		{{ range $i, $chart := .Charts }}
			<div id="chart-{{ $i }}"></div>
		{{ end }}
		<script>
			$(function () {
				{{ range $i, $chart := .Charts }}
					Highcharts.chart('chart-{{ $i }}', {
						chart: {
							type: '{{ $chart.Type }}'
						},
						title: {
							text: '{{ $chart.Title }}'
						},
						subtitle: {
							text: '{{ $chart.Subtitle }}'
						},
					{{ if ne $chart.XAxis "" }}
						xAxis: {
							title: {
								enabled: true,
								text: '{{ $chart.XAxis }}'
							}
						},
					{{ end }}
					{{ if ne $chart.YAxis "" }}
						yAxis: {
							title: {
								enabled: true,
								text: '{{ $chart.YAxis }}'
							}
						},
					{{ end }}
						series: [
					{{ range $series := $chart.Series }}
							{
								name: '{{ $series.Name }}',
								color: 'rgba({{ $series.Color.R }}, {{ $series.Color.G }}, {{ $series.Color.B }}, {{ $series.Color.A }})',
								data: {{ $series.Data }}
							},
					{{ end }}
						],
					});
				{{ end }}
			});
		</script>
	</body>
</html>
`

const tplName = "highcharts"

var tpl = template.Must(template.New(tplName).Parse(tplStr))

// TempFilePrefix is a prefix of a temporary file.
var TempFilePrefix = "highcharts"

// Page represents a page of charts.
type Page struct {
	Charts []*Chart
}

// Print prints the page to a temp file.
func (p *Page) Print(callback func(*Page, *os.File) error) error {
	f, err := ioutil.TempFile("", TempFilePrefix)
	if err != nil {
		return err
	}

	return p.PrintFile(f, callback)
}

// PrintFile prints the page to the file.
func (p *Page) PrintFile(f *os.File, callback func(*Page, *os.File) error) (err error) {
	defer func() {
		if err == nil && callback != nil {
			err = callback(p, f)
		}
	}()

	defer f.Close()

	err = p.WriteTo(f)

	return
}

// WriteTo writes the page to the writer.
func (p *Page) WriteTo(w io.Writer) error {
	return tpl.Execute(w, p)
}

// AppendChart appends the chart to the page.
func (p *Page) AppendChart(c *Chart) {
	p.Charts = append(p.Charts, c)
}

// NewPage creates and returns a page.
func NewPage() *Page {
	return &Page{
		Charts: make([]*Chart, 0),
	}
}
