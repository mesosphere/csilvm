// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file is based on
// https://github.com/uber-go/tally/blob/1ed35a14802a3210727a56a9b8089f04c959d8a4/statsd/reporter.go
// It has been modified to wrap
// https://github.com/DataDog/datadog-go/blob/40bafcb5f6c1d49df36deaf4ab019e44961d5e36/statsd/statsd.go
// instead of the go-statsd-client.

package ddstatsd

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/uber-go/tally"
)

const (
	// DefaultHistogramBucketNamePrecision is the default
	// precision to use when formatting the metric name
	// with the histogram bucket bound values.
	DefaultHistogramBucketNamePrecision = uint(6)
)

type statsReporter struct {
	client     *statsd.Client
	sampleRate float64
	bucketFmt  string
}

// Options is a set of options for the tally reporter.
type Options struct {
	// SampleRate is the metrics emission sample rate. If you
	// do not set this value it will be set to 1.
	SampleRate float32

	// HistogramBucketNamePrecision is the precision to use when
	// formatting the metric name with the histogram bucket bound values.
	// By default this will be set to the const DefaultHistogramBucketPrecision.
	HistogramBucketNamePrecision uint
}

// NewReporter wraps a *statsd.Client for use with tally. Use either
// statsd.New or statsd.NewBuffered.
func NewReporter(statsd *statsd.Client, opts Options) tally.StatsReporter {
	var nilSampleRate float32
	if opts.SampleRate == nilSampleRate {
		opts.SampleRate = 1.0
	}
	if opts.HistogramBucketNamePrecision == 0 {
		opts.HistogramBucketNamePrecision = DefaultHistogramBucketNamePrecision
	}
	return &statsReporter{
		client:     statsd,
		sampleRate: float64(opts.SampleRate),
		bucketFmt:  "%." + strconv.Itoa(int(opts.HistogramBucketNamePrecision)) + "f",
	}
}

func (r *statsReporter) ReportCounter(name string, tags map[string]string, value int64) {
	tagsList := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, k+":"+v)
	}
	r.client.Count(name, value, tagsList, r.sampleRate)
}

func (r *statsReporter) ReportGauge(name string, tags map[string]string, value float64) {
	tagsList := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, k+":"+v)
	}
	r.client.Gauge(name, value, tagsList, r.sampleRate)
}

func (r *statsReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	tagsList := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, k+":"+v)
	}
	r.client.Timing(name, interval, tagsList, r.sampleRate)
}

func (r *statsReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
	tagsList := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, k+":"+v)
	}
	r.client.Histogram(
		fmt.Sprintf("%s.%s-%s", name,
			r.valueBucketString(bucketLowerBound),
			r.valueBucketString(bucketUpperBound)),
		float64(samples), tagsList, r.sampleRate)
}

func (r *statsReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
	tagsList := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsList = append(tagsList, k+":"+v)
	}
	r.client.Histogram(
		fmt.Sprintf("%s.%s-%s", name,
			r.durationBucketString(bucketLowerBound),
			r.durationBucketString(bucketUpperBound)),
		float64(samples), tagsList, r.sampleRate)
}

func (r *statsReporter) valueBucketString(
	upperBound float64,
) string {
	if upperBound == math.MaxFloat64 {
		return "infinity"
	}
	if upperBound == -math.MaxFloat64 {
		return "-infinity"
	}
	return fmt.Sprintf(r.bucketFmt, upperBound)
}

func (r *statsReporter) durationBucketString(
	upperBound time.Duration,
) string {
	if upperBound == time.Duration(math.MaxInt64) {
		return "infinity"
	}
	if upperBound == time.Duration(math.MinInt64) {
		return "-infinity"
	}
	return upperBound.String()
}

func (r *statsReporter) Capabilities() tally.Capabilities {
	return r
}

func (r *statsReporter) Reporting() bool {
	return true
}

func (r *statsReporter) Tagging() bool {
	return true
}

func (r *statsReporter) Flush() {
	if err := r.client.Flush(); err != nil {
		log.Printf("failed to flush metrics: err=%v", err)
	}
}
