//go:build go1.25

package self_test

import tls "github.com/Mag1cFall/utls" // [uQUIC]

func getCurveID(connState tls.ConnectionState) tls.CurveID {
	return connState.CurveID
}
