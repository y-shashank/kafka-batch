package metrics

import "errors"

var errNoSink = errors.New("metrics enabled but no statsd_addr or handler configured")
