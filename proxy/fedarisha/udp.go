package fedarisha

import (
	"encoding/binary"
	"io"

	"github.com/xtls/xray-core/common/buf"
	xraynet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/xudp"
)

const targetHeaderUDPFlag uint16 = 0x8000

type fedarishaPacketWriter struct {
	writer buf.Writer
	dest   xraynet.Destination
}

func newFedarishaPacketWriter(writer buf.Writer, dest xraynet.Destination) *fedarishaPacketWriter {
	return &fedarishaPacketWriter{
		writer: writer,
		dest:   dest,
	}
}

func (w *fedarishaPacketWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)

	frames := make(buf.MultiBuffer, 0, len(mb))
	for _, payload := range mb {
		if payload.IsEmpty() {
			continue
		}

		frame := buf.New()
		dest := w.dest
		if payload.UDP != nil {
			dest = *payload.UDP
		}

		if dest.Network == xraynet.Network_UDP {
			if err := writePacketDestination(frame, dest); err != nil {
				frame.Release()
				return err
			}
		} else {
			frame.Write([]byte{0, 0})
		}

		if err := binary.Write(frame, binary.BigEndian, uint16(payload.Len())); err != nil {
			frame.Release()
			return err
		}
		frame.Write(payload.Bytes())
		frames = append(frames, frame)
	}

	if frames.IsEmpty() {
		return nil
	}
	return w.writer.WriteMultiBuffer(frames)
}

type fedarishaPacketReader struct {
	reader      io.Reader
	defaultDest xraynet.Destination
}

func newFedarishaPacketReader(reader io.Reader, defaultDest xraynet.Destination) *fedarishaPacketReader {
	return &fedarishaPacketReader{
		reader:      reader,
		defaultDest: defaultDest,
	}
}

func (r *fedarishaPacketReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r.reader, lenBuf[:]); err != nil {
		return nil, err
	}

	metaLen := binary.BigEndian.Uint16(lenBuf[:])
	var dest *xraynet.Destination
	if metaLen > 0 {
		meta := buf.New()
		if _, err := meta.ReadFullFrom(r.reader, int32(metaLen)); err != nil {
			meta.Release()
			return nil, err
		}
		parsed, err := readPacketDestination(meta)
		meta.Release()
		if err != nil {
			return nil, err
		}
		dest = parsed
	} else if r.defaultDest.Network == xraynet.Network_UDP {
		defaultDest := r.defaultDest
		dest = &defaultDest
	}

	if _, err := io.ReadFull(r.reader, lenBuf[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint16(lenBuf[:])
	if payloadLen == 0 || int32(payloadLen) > buf.Size {
		return nil, io.ErrUnexpectedEOF
	}

	payload := buf.New()
	if _, err := payload.ReadFullFrom(r.reader, int32(payloadLen)); err != nil {
		payload.Release()
		return nil, err
	}
	payload.UDP = dest

	return buf.MultiBuffer{payload}, nil
}

func writePacketDestination(frame *buf.Buffer, dest xraynet.Destination) error {
	meta := buf.New()
	defer meta.Release()

	meta.WriteByte(byte(2))
	if err := xudp.AddrParser.WriteAddressPort(meta, dest.Address, dest.Port); err != nil {
		return err
	}

	if err := binary.Write(frame, binary.BigEndian, uint16(meta.Len())); err != nil {
		return err
	}
	frame.Write(meta.Bytes())
	return nil
}

func readPacketDestination(meta *buf.Buffer) (*xraynet.Destination, error) {
	if meta.IsEmpty() {
		return nil, nil
	}

	network := meta.Byte(0)
	meta.Advance(1)

	addr, port, err := xudp.AddrParser.ReadAddressPort(nil, meta)
	if err != nil {
		return nil, err
	}
	if network != 2 {
		return nil, nil
	}

	dest := xraynet.UDPDestination(addr, port)
	return &dest, nil
}
