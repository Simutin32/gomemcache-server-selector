package selector

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/foxcpp/go-mockdns"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net"
	"strings"
	"testing"
)

var (
	testHost         = "memcache.test.com"
	lowVersionPrefix = []byte("version")
	dnsLogger        = &fakeDnsLogger{}
)

func BenchmarkPickServer(b *testing.B) {
	// at least two to avoid 0 and 1 special cases:
	benchPickServer(b, "127.0.0.1:1234", "127.0.0.1:1235")
}

func BenchmarkPickServer_Single(b *testing.B) {
	benchPickServer(b, "127.0.0.1:1234")
}

func benchPickServer(b *testing.B, servers ...string) {
	b.ReportAllocs()
	ss := NewServerList(zap.NewNop(), servers...)
	for i := 0; i < b.N; i++ {
		if _, err := ss.PickServer("some key"); err != nil {
			b.Fatal(err)
		}
	}
}

// TestDnsIpChangedCustomServerSelector тест кастомного сервер селектора
func TestDnsIpChangedCustomServerSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zap.NewNop()

	// стартуем 2 фейк мемкеша по разным адресам, но с одинаковым первым свободным портом
	fakeServer1 := createFakeMemcache(ctx, "tcp", "127.0.0.1:0", logger)

	port := getPort(fakeServer1.Addr())
	fakeServer2 := createFakeMemcache(ctx, "tcp", "[::1]:"+port, logger)
	defer fakeServer2.Close()

	// патчим дефолтный dns resolver чтобы по тестовому хосту резолвился сначала ТОЛЬКО сервер №1
	srv, err := mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		testHost + ".": {A: []string{"127.0.0.1"}},
	}, dnsLogger, false)
	if err != nil {
		logger.Fatal("can't create dns", zap.Error(err))
	}
	defer srv.Close()
	srv.PatchNet(net.DefaultResolver)
	// Important if net.DefaultResolver is modified.
	defer mockdns.UnpatchNet(net.DefaultResolver)

	// инитим мемкеш
	ss := NewServerList(logger, testHost+":"+port)
	mc := memcache.NewFromSelector(ss)

	// пингуем, ожидаем что ошибок нет
	assert.NoError(t, mc.Ping())

	// закрываем фейковый сервер чтобы убедить что запрос на него не придет
	fakeServer1.Close()

	//переписываем dns чтобы тот же хост смотрел уже на сервер №2
	srv, err = mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		testHost + ".": {AAAA: []string{"::1"}},
	}, dnsLogger, false)
	if err != nil {
		logger.Fatal("can't update dns", zap.Error(err))
	}
	defer srv.Close()

	srv.PatchNet(net.DefaultResolver)

	// первый запрос после смены dns отваливается по timeout ТК либа пишет себе локальный кэш ip -> хост
	// но после фейла кэш потрется и на следующий запрос мы получим новый ip для коннекта
	assert.ErrorContains(t, mc.Ping(), "i/o timeout")

	// пингуем, ожидаем что ошибок нет, хотя IP поменялся
	assert.NoError(t, mc.Ping())
}

// TestDnsIpChangedDefaultServerSelector тест сервер селектора по умолчанию, убеждаемся что он не может менять IP при смене DNS
func TestDnsIpChangedDefaultServerSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zap.NewNop()

	// стартуем 2 фейк мемкеша по разным адресам, но с одинаковым первым свободным портом
	fakeServer1 := createFakeMemcache(ctx, "tcp", "127.0.0.1:0", logger)

	port := getPort(fakeServer1.Addr())
	fakeServer2 := createFakeMemcache(ctx, "tcp", "[::1]:"+port, logger)
	defer fakeServer2.Close()

	// патчим дефолтный dns resolver чтобы по тестовому хосту резолвился сначала ТОЛЬКО сервер №1
	srv, err := mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		testHost + ".": {A: []string{"127.0.0.1"}},
	}, dnsLogger, false)
	if err != nil {
		logger.Fatal("can't create dns", zap.Error(err))
	}
	defer srv.Close()
	srv.PatchNet(net.DefaultResolver)
	// Important if net.DefaultResolver is modified.
	defer mockdns.UnpatchNet(net.DefaultResolver)

	// инитим мемкеш СТАРЫМ способом, с сервер селектором по умолчанию
	mc := memcache.New(testHost + ":" + port)

	// пингуем, ожидаем что ошибок нет
	assert.NoError(t, mc.Ping())

	// закрываем фейковый сервер чтобы убедить что запрос на него не придет
	fakeServer1.Close()

	//переписываем dns чтобы тот же хост смотрел уже на сервер №2
	srv, err = mockdns.NewServerWithLogger(map[string]mockdns.Zone{
		testHost + ".": {AAAA: []string{"::1"}},
	}, dnsLogger, false)
	if err != nil {
		logger.Fatal("can't update dns", zap.Error(err))
	}
	defer srv.Close()

	srv.PatchNet(net.DefaultResolver)

	// первый запрос после смены dns отваливается по timeout ТК либа пишет себе локальный кэш ip -> хост
	// но после фейла кэш потрется и на следующий запрос мы получим новый ip для коннекта
	assert.ErrorContains(t, mc.Ping(), "i/o timeout")

	// пингуем, ожидаем ошибку, тк стандартый сервер селектор не умеет заменять IP при смене DNS
	// и будет пробовать долбиться в выключенный fakeServer1
	assert.ErrorContains(t, mc.Ping(), "connection refused")
}

func createFakeMemcache(ctx context.Context, network, addr string, logger *zap.Logger) net.Listener {
	var lc net.ListenConfig
	fakeServer, err := lc.Listen(ctx, network, addr)
	if err != nil {
		logger.Fatal("Could not open fake server: ", zap.Error(err))
	}
	go func() {
		for {
			if c, err := fakeServer.Accept(); err == nil {
				go func(connect net.Conn) {
					rw := bufio.NewReadWriter(bufio.NewReader(connect), bufio.NewWriter(connect))
					bar, err := rw.ReadSlice('\n')
					if err != nil {
						logger.Fatal("can't read bytes", zap.Error(err))
					}

					if bytes.HasPrefix(bar, lowVersionPrefix) {
						if _, err = fmt.Fprintf(rw, "VERSION 1.6.15\n"); err != nil {
							logger.Fatal("can't write to rw", zap.Error(err))
						}

						err = rw.Flush()
						if err != nil {
							logger.Fatal("can't flush rw")
						}

						logger.Debug("got ping",
							zap.String("local_addr", connect.LocalAddr().String()),
							zap.String("remote_addr", connect.RemoteAddr().String()),
						)
					}
				}(c)
			} else {
				return
			}
		}
	}()

	return fakeServer
}

type fakeDnsLogger struct{}

func (f *fakeDnsLogger) Printf(_ string, _ ...interface{}) {}

func getPort(addr net.Addr) string {
	return strings.Split(addr.String(), ":")[1]
}
