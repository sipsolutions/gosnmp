package gosnmp

import (
	"context"
	"fmt"
)

type snmpResult struct {
	pkt *SnmpPacket
	err error
}

// DialWithCtx connects through udp with a context
func (x *GoSNMP) DialWithCtx(ctx context.Context) error {
	errCh := make(chan error)
	go func() {
		errCh <- x.connect("udp")
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("request cancelled while connecting")
	case err := <-errCh:
		return err
	}
}

// GetWithCtx calls Get with a context
func (x *GoSNMP) GetWithCtx(ctx context.Context, oids []string) (result *SnmpPacket, err error) {
	snmpRes := make(chan snmpResult)
	go func() {
		pkt, err := x.Get(oids)
		snmpRes <- snmpResult{pkt, err}
	}()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("snmp get cancelled")
	case res := <-snmpRes:
		return res.pkt, res.err
	}
}

// BulkWalkWithCtx calls BulkWalk with a context
func (x *GoSNMP) BulkWalkWithCtx(ctx context.Context, rootOid string, walkFn WalkFunc) error {
	errCh := make(chan error)
	go func() {
		errCh <- x.BulkWalk(rootOid, walkFn)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("snmp bulk walk cancelled")
	case err := <-errCh:
		return err
	}
	return nil
}

// WalkWithCtx calls Walk with a context
func (x *GoSNMP) WalkWithCtx(ctx context.Context, rootOid string, walkFn WalkFunc) error {
	errCh := make(chan error)
	go func() {
		errCh <- x.Walk(rootOid, walkFn)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("snmp walk cancelled")
	case err := <-errCh:
		return err
	}
	return nil
}
