package quic

import (
	"testing"
	"time"

	"github.com/Mag1cFall/uquic/internal/monotime"
	"github.com/Mag1cFall/uquic/internal/protocol"
	"github.com/Mag1cFall/uquic/internal/synctest"
	"github.com/Mag1cFall/uquic/internal/wire"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestConnectionKeepAliveStreamsOnly 验证 KeepAliveStreamsOnly 只在本端双向流尚未完成时发送保活 PING，没有流时连接按空闲超时关闭
func TestConnectionKeepAliveStreamsOnly(t *testing.T) {
	t.Run("open stream", func(t *testing.T) {
		testConnectionKeepAliveStreamsOnly(t, true)
	})

	t.Run("no stream", func(t *testing.T) {
		testConnectionKeepAliveStreamsOnly(t, false)
	})
}

func testConnectionKeepAliveStreamsOnly(t *testing.T, openStream bool) {
	synctest.Test(t, func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		unpacker := NewMockUnpacker(mockCtrl)
		tc := newServerTestConnection(t,
			mockCtrl,
			&Config{MaxIdleTimeout: time.Second, KeepAlivePeriod: time.Second, KeepAliveStreamsOnly: true},
			false,
			connectionOptUnpacker(unpacker),
			connectionOptHandshakeConfirmed(),
			connectionOptRTT(time.Millisecond),
		)
		const idleTimeout = 50 * time.Millisecond
		require.NoError(t, tc.conn.handleTransportParameters(&wire.TransportParameters{
			MaxIdleTimeout:   idleTimeout,
			MaxBidiStreamNum: 1,
		}))
		if openStream {
			_, err := tc.conn.OpenStream()
			require.NoError(t, err)
		}

		buf := getPacketBuffer()
		var err error
		buf.Data, err = wire.AppendShortHeader(buf.Data, tc.srcConnID, 1, protocol.PacketNumberLen1, protocol.KeyPhaseZero)
		require.NoError(t, err)
		buf.Data = append(buf.Data, []byte("packet")...)

		errChan := make(chan error, 1)
		go func() { errChan <- tc.conn.run() }()

		var unpackTime, packTime monotime.Time
		done := make(chan struct{})
		unpacker.EXPECT().UnpackShortHeader(gomock.Any(), gomock.Any()).DoAndReturn(
			func(t monotime.Time, bytes []byte) (protocol.PacketNumber, protocol.PacketNumberLen, protocol.KeyPhaseBit, []byte, error) {
				unpackTime = monotime.Now()
				return protocol.PacketNumber(1), protocol.PacketNumberLen1, protocol.KeyPhaseZero, []byte{0}, nil
			},
		)
		tc.packer.EXPECT().AppendPacket(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(shortHeaderPacket{}, errNothingToPack)

		if openStream {
			tc.packer.EXPECT().AppendPacket(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(buffer *packetBuffer, count protocol.ByteCount, t monotime.Time, version protocol.Version) (shortHeaderPacket, error) {
					packTime = monotime.Now()
					close(done)
					return shortHeaderPacket{}, errNothingToPack
				},
			)
			tc.conn.handlePacket(receivedPacket{data: buf.Data, buffer: buf, rcvTime: monotime.Now(), remoteAddr: tc.remoteAddr})
			select {
			case <-done:
				require.Equal(t, unpackTime.Add(idleTimeout/2), packTime)
			case <-time.After(idleTimeout):
				t.Fatal("timeout")
			}
			tc.connRunner.EXPECT().Remove(gomock.Any()).AnyTimes()
			tc.conn.destroy(nil)
		} else {
			tc.connRunner.EXPECT().Remove(gomock.Any()).AnyTimes()
			tc.conn.handlePacket(receivedPacket{data: buf.Data, buffer: buf, rcvTime: monotime.Now(), remoteAddr: tc.remoteAddr})
		}

		synctest.Wait()

		select {
		case err := <-errChan:
			if openStream {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, &IdleTimeoutError{})
			}
		case <-time.After(time.Hour):
			t.Fatal("timeout")
		}
	})
}
