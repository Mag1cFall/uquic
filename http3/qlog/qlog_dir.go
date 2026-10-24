package qlog

import (
	"context"

	"github.com/Mag1cFall/uquic"
	"github.com/Mag1cFall/uquic/qlog"
	"github.com/Mag1cFall/uquic/qlogwriter"
)

const EventSchema = "urn:ietf:params:qlog:events:http3-12"

func DefaultConnectionTracer(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
	return qlog.DefaultConnectionTracerWithSchemas(ctx, isClient, connID, []string{qlog.EventSchema, EventSchema})
}
