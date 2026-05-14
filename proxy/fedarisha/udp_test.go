package fedarisha

import (
	"bytes"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	xraynet "github.com/xtls/xray-core/common/net"
)

func TestFedarishaPacketFrameDefaultDestination(t *testing.T) {
	dest := xraynet.UDPDestination(xraynet.DomainAddress("example.com"), 53)
	var raw bytes.Buffer

	payload := buf.New()
	payload.Write([]byte("dns-query"))

	writer := newFedarishaPacketWriter(buf.NewWriter(&raw), dest)
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{payload}); err != nil {
		t.Fatal(err)
	}

	reader := newFedarishaPacketReader(&raw, dest)
	mb, err := reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)

	if len(mb) != 1 {
		t.Fatalf("expected 1 buffer, got %d", len(mb))
	}
	if string(mb[0].Bytes()) != "dns-query" {
		t.Fatalf("unexpected payload %q", string(mb[0].Bytes()))
	}
	if mb[0].UDP == nil || mb[0].UDP.NetAddr() != dest.NetAddr() {
		t.Fatalf("unexpected UDP destination: %#v", mb[0].UDP)
	}
}

func TestFedarishaPacketFrameExplicitDestination(t *testing.T) {
	defaultDest := xraynet.UDPDestination(xraynet.DomainAddress("default.example"), 53)
	packetDest := xraynet.UDPDestination(xraynet.DomainAddress("packet.example"), 123)
	var raw bytes.Buffer

	payload := buf.New()
	payload.Write([]byte("payload"))
	payload.UDP = &packetDest

	writer := newFedarishaPacketWriter(buf.NewWriter(&raw), defaultDest)
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{payload}); err != nil {
		t.Fatal(err)
	}

	reader := newFedarishaPacketReader(&raw, defaultDest)
	mb, err := reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)

	if len(mb) != 1 {
		t.Fatalf("expected 1 buffer, got %d", len(mb))
	}
	if mb[0].UDP == nil || mb[0].UDP.NetAddr() != packetDest.NetAddr() {
		t.Fatalf("unexpected UDP destination: %#v", mb[0].UDP)
	}
}
