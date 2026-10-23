package gateway

import (
	"net"
	"net/http"
	"testing"
)

func TestRemoteIP_NoTrustedProxies(t *testing.T) {
	r, _ := http.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.2.3.4:12345"
	r.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.1")

	got := remoteIP(r, nil)
	if got != "1.2.3.4" {
		t.Errorf("expected peer IP 1.2.3.4, got %s", got)
	}
}

func TestRemoteIP_TrustedProxy(t *testing.T) {
	_, proxyNet, _ := net.ParseCIDR("192.168.0.0/16")
	trusted := []*net.IPNet{proxyNet}

	r, _ := http.NewRequest("GET", "/", nil)
	r.RemoteAddr = "192.168.1.1:12345"
	r.Header.Set("X-Forwarded-For", "8.8.8.8, 192.168.1.1")

	got := remoteIP(r, trusted)
	if got != "8.8.8.8" {
		t.Errorf("expected 8.8.8.8, got %s", got)
	}
}

func TestRemoteIP_UntrustedPeerIgnoresXFF(t *testing.T) {
	_, proxyNet, _ := net.ParseCIDR("10.0.0.0/8")
	trusted := []*net.IPNet{proxyNet}

	r, _ := http.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.2.3.4:12345"
	r.Header.Set("X-Forwarded-For", "spoofed-ip")

	got := remoteIP(r, trusted)
	if got != "1.2.3.4" {
		t.Errorf("expected peer IP 1.2.3.4, got %s", got)
	}
}

func TestParseTrustedProxies(t *testing.T) {
	nets := ParseTrustedProxies([]string{"10.0.0.0/8", "192.168.1.1", "invalid"})
	if len(nets) != 2 {
		t.Fatalf("expected 2 nets, got %d", len(nets))
	}
}
