package quic

import (
	"testing"
	"time"

	"github.com/Mag1cFall/uquic/internal/monotime"
	"github.com/Mag1cFall/uquic/internal/wire"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestConnectionKeepAliveStreamsOnly 验证 KeepAliveStreamsOnly 的保活时刻：没有双向流或有待发数据时不保活，否则从空闲计时起点经过空闲超时的一半
func TestConnectionKeepAliveStreamsOnly(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	tc := newServerTestConnection(t,
		mockCtrl,
		&Config{MaxIdleTimeout: time.Second, KeepAlivePeriod: time.Second, KeepAliveStreamsOnly: true},
		false,
		connectionOptHandshakeConfirmed(),
		connectionOptRTT(time.Millisecond),
	)
	const idleTimeout = 50 * time.Millisecond
	require.NoError(t, tc.conn.handleTransportParameters(&wire.TransportParameters{
		MaxIdleTimeout:   idleTimeout,
		MaxBidiStreamNum: 1,
	}))
	received := monotime.Now()
	tc.conn.lastPacketReceivedTime = received
	require.Zero(t, tc.conn.nextKeepAliveTime())

	_, err := tc.conn.OpenStream()
	require.NoError(t, err)
	require.Equal(t, received.Add(idleTimeout/2), tc.conn.nextKeepAliveTime())

	sent := received.Add(20 * time.Millisecond)
	tc.conn.firstAckElicitingPacketAfterIdleSentTime = sent
	require.Equal(t, sent.Add(idleTimeout/2), tc.conn.nextKeepAliveTime())

	tc.conn.framer.QueueControlFrame(&wire.MaxDataFrame{MaximumData: 1})
	require.Zero(t, tc.conn.nextKeepAliveTime())
}
