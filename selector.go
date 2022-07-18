package selector

import (
	"errors"
	"hash/crc32"
	"net"
	"strings"
	"sync"

	_ "github.com/bradfitz/gomemcache/memcache"
	"go.uber.org/zap"
)

var (
	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

type ServerList struct {
	addrs []net.Addr
}

// staticAddr caches the Network() and String() values from any net.Addr.
type staticAddr struct {
	ntw, str string
}

func newAddrFromString(addr string) net.Addr {
	return &staticAddr{
		ntw: "tcp",
		str: addr,
	}
}

func (a *staticAddr) Network() string { return a.ntw }
func (a *staticAddr) String() string  { return a.str }

func (s *ServerList) NewServerList(logger *zap.Logger, servers ...string) *ServerList {
	naddr := make([]net.Addr, 0, len(servers))
	for _, server := range servers {
		if strings.Contains(server, "/") {
			_, err := net.ResolveUnixAddr("unix", server)
			if err != nil {
				logger.Fatal("can't resolve unix addr", zap.Error(err))
			}
			naddr = append(naddr, newAddrFromString(server))
		} else {
			_, err := net.ResolveTCPAddr("tcp", server)
			if err != nil {
				logger.Fatal("can't resolve tcp addr", zap.Error(err))
			}
			naddr = append(naddr, newAddrFromString(server))
		}
	}

	return &ServerList{
		addrs: naddr,
	}
}

// Each iterates over each server calling the given function
func (s *ServerList) Each(f func(net.Addr) error) error {
	for _, a := range s.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// keyBufPool returns []byte buffers for use by PickServer's call to
// crc32.ChecksumIEEE to avoid allocations. (but doesn't avoid the
// copies, which at least are bounded in size and small)
var keyBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

func (s *ServerList) PickServer(key string) (net.Addr, error) {
	if len(s.addrs) == 0 {
		return nil, ErrNoServers
	}
	if len(s.addrs) == 1 {
		return s.addrs[0], nil
	}
	bufp := keyBufPool.Get().(*[]byte)
	n := copy(*bufp, key)
	cs := crc32.ChecksumIEEE((*bufp)[:n])
	keyBufPool.Put(bufp)

	return s.addrs[cs%uint32(len(s.addrs))], nil
}
