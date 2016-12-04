package ml

import "github.com/keijiyoshida/gds/vector"

// Perceptron represents a perceptron.
type Perceptron struct {
	W         []float64
	NumErrors []int
	eta       float64
	numIter   int
}

// Fit trains the perceptron by using the input training data.
func (p *Perceptron) Fit(x [][]float64, y []float64) {
	p.W = make([]float64, len(x[0])+1)
	p.NumErrors = make([]int, p.numIter)

	for i := 0; i < p.numIter; i++ {
		numError := 0

		for j := 0; j < len(x); j++ {
			d := p.eta * (y[j] - p.Predict(x[j]))

			p.W[0] += d

			for k := 0; k < len(x[0]); k++ {
				p.W[k+1] += d * x[j][k]
			}
			if d != 0 {
				numError++
			}
		}

		p.NumErrors[i] = numError
	}
}

// Predict predicts a labeled value and returns it.
func (p *Perceptron) Predict(x []float64) float64 {
	if vector.DotProduct(x, p.W[1:])+p.W[0] >= 0.0 {
		return 1.0
	}

	return -1.0
}

// NewPerceptron creates and returns a perceptron.
func NewPerceptron(eta float64, numIter int) *Perceptron {
	return &Perceptron{
		eta:     eta,
		numIter: numIter,
	}
}
