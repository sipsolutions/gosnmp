package gosnmp

// Copyright 2012-2016 The GoSNMP Authors. All rights reserved.  Use of this
// source code is governed by a BSD-style license that can be found in the
// LICENSE file.

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"sync/atomic"
)

// SnmpV3MsgFlags contains various message flags to describe Authentication, Privacy, and whether a report PDU must be sent.
type SnmpV3MsgFlags uint8

// Possible values of SnmpV3MsgFlags
const (
	NoAuthNoPriv SnmpV3MsgFlags = 0x0 // No authentication, and no privacy
	AuthNoPriv   SnmpV3MsgFlags = 0x1 // Authentication and no privacy
	AuthPriv     SnmpV3MsgFlags = 0x3 // Authentication and privacy
	Reportable   SnmpV3MsgFlags = 0x4 // Report PDU must be sent.
)

// SnmpV3SecurityModel describes the security model used by a SnmpV3 connection
type SnmpV3SecurityModel uint8

// UserSecurityModel is the only SnmpV3SecurityModel currently implemented.
const (
	UserSecurityModel SnmpV3SecurityModel = 3
)

// SnmpV3AuthProtocol describes the authentication protocol in use by an authenticated SnmpV3 connection.
type SnmpV3AuthProtocol uint8

// NoAuth, MD5, and SHA are implemented
const (
	NoAuth SnmpV3AuthProtocol = 1
	MD5    SnmpV3AuthProtocol = 2
	SHA    SnmpV3AuthProtocol = 3
)

// SnmpV3PrivProtocol is the privacy protocol in use by an private SnmpV3 connection.
type SnmpV3PrivProtocol uint8

// NoPriv, DES implemented, AES planned
const (
	NoPriv SnmpV3PrivProtocol = 1
	DES    SnmpV3PrivProtocol = 2
	AES    SnmpV3PrivProtocol = 3
)

// SnmpV3SecurityParameters is a generic interface type to contain various implementations of SnmpV3SecurityParameters
type SnmpV3SecurityParameters interface {
	Copy() SnmpV3SecurityParameters
}

// UsmSecurityParameters is an implementation of SnmpV3SecurityParameters for the UserSecurityModel
type UsmSecurityParameters struct {
	AuthoritativeEngineID    string
	AuthoritativeEngineBoots uint32
	AuthoritativeEngineTime  uint32
	UserName                 string
	AuthenticationParameters string
	PrivacyParameters        []byte

	AuthenticationProtocol SnmpV3AuthProtocol
	PrivacyProtocol        SnmpV3PrivProtocol

	AuthenticationPassphrase string
	PrivacyPassphrase        string

	localDESSalt uint32
	localAESSalt uint64
}

// Copy method for UsmSecurityParameters used to copy a SnmpV3SecurityParameters without knowing it's implementation
func (sp *UsmSecurityParameters) Copy() SnmpV3SecurityParameters {
	return &UsmSecurityParameters{AuthoritativeEngineID: sp.AuthoritativeEngineID,
		AuthoritativeEngineBoots: sp.AuthoritativeEngineBoots,
		AuthoritativeEngineTime:  sp.AuthoritativeEngineTime,
		UserName:                 sp.UserName,
		AuthenticationParameters: sp.AuthenticationParameters,
		PrivacyParameters:        sp.PrivacyParameters,
		AuthenticationProtocol:   sp.AuthenticationProtocol,
		PrivacyProtocol:          sp.PrivacyProtocol,
		AuthenticationPassphrase: sp.AuthenticationPassphrase,
		PrivacyPassphrase:        sp.PrivacyPassphrase,
		localDESSalt:             sp.localDESSalt,
		localAESSalt:             sp.localAESSalt,
	}
}

func (x *GoSNMP) validateParametersV3() error {
	if x.SecurityModel != UserSecurityModel {
		return fmt.Errorf("The SNMPV3 User Security Model is the only SNMPV3 security model currently implemented")
	}

	// fix if you need to implement a new security model
	var usm *UsmSecurityParameters
	var err error
	if usm, err = castUsmSecParams(x.SecurityParameters); err != nil {
		return fmt.Errorf("The SecurityParameters field does not contain a populated instance of UsmSecurityParameters")
	}

	securityLevel := x.MsgFlags & AuthPriv

	switch securityLevel {
	case AuthPriv:
		if usm.PrivacyProtocol <= NoPriv {
			return fmt.Errorf("SecurityParameters.PrivacyProtocol is required")
		}
		if usm.PrivacyPassphrase == "" {
			return fmt.Errorf("SecurityParameters.PrivacyPassphrase is required")
		}
		fallthrough
	case AuthNoPriv:
		if usm.AuthenticationProtocol <= NoAuth {
			return fmt.Errorf("SecurityParameters.AuthenticationProtocol is required")
		}
		if usm.AuthenticationPassphrase == "" {
			return fmt.Errorf("SecurityParameters.AuthenticationPassphrase is required")
		}
		fallthrough
	case NoAuthNoPriv:
		if usm.UserName == "" {
			return fmt.Errorf("SecurityParameters.UserName is required")
		}
	default:
		return fmt.Errorf("MsgFlags must be populated with an appropriate security level")
	}

	return nil
}

func (x *GoSNMP) initSalt() error {
	var err error
	if x.SecurityModel == UserSecurityModel {
		var secParams *UsmSecurityParameters

		if secParams, err = castUsmSecParams(x.SecurityParameters); err != nil {
			return err
		}

		switch secParams.PrivacyProtocol {
		case AES:
			salt := make([]byte, 8)
			_, err = crand.Read(salt)
			if err != nil {
				return fmt.Errorf("Error creating a cryptographically secure salt: %s\n", err.Error())
			}
			secParams.localAESSalt = binary.BigEndian.Uint64(salt)
		case DES:
			salt := make([]byte, 4)
			_, err = crand.Read(salt)
			if err != nil {
				return fmt.Errorf("Error creating a cryptographically secure salt: %s\n", err.Error())
			}
			secParams.localDESSalt = binary.BigEndian.Uint32(salt)
		}
	}
	return nil
}

// authenticate the marshalled result of a snmp version 3 packet
func (packet *SnmpPacket) authenticate(msg []byte, authParamStart uint32) ([]byte, error) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Printf("recover: %v\n", e)
		}
	}()
	if packet.Version != Version3 {
		return msg, nil
	}
	if packet.MsgFlags&AuthNoPriv == 0 {
		return msg, nil
	}
	if packet.SecurityModel != UserSecurityModel {
		return nil, fmt.Errorf("Error authenticating message: Unknown security model.")
	}

	var secParams *UsmSecurityParameters
	var err error

	if secParams, err = castUsmSecParams(packet.SecurityParameters); err != nil {
		return nil, err
	}
	var secretKey = genlocalkey(secParams.AuthenticationProtocol,
		secParams.AuthenticationPassphrase,
		secParams.AuthoritativeEngineID)

	var extkey [64]byte

	copy(extkey[:], secretKey)

	var k1, k2 [64]byte

	for i := 0; i < 64; i++ {
		k1[i] = extkey[i] ^ 0x36
		k2[i] = extkey[i] ^ 0x5c
	}

	var h, h2 hash.Hash

	switch secParams.AuthenticationProtocol {
	default:
		h = md5.New()
		h2 = md5.New()
	case SHA:
		h = sha1.New()
		h2 = sha1.New()
	}

	h.Write(k1[:])
	h.Write(msg)
	d1 := h.Sum(nil)
	h2.Write(k2[:])
	h2.Write(d1)
	copy(msg[authParamStart:authParamStart+12], h2.Sum(nil)[:12])
	return msg, nil
}

func (x *GoSNMP) testUsmAuthentication(packet []byte, result *SnmpPacket) error {
	if x.Version != Version3 {
		return fmt.Errorf("testUsmAuthentication called with non Version3 connection")
	}

	if x.SecurityModel != UserSecurityModel {
		return fmt.Errorf("testUsmAuthentication called with connection that is not using the User Security Model")
	}

	var secParameters *UsmSecurityParameters
	var err error

	if secParameters, err = castUsmSecParams(x.SecurityParameters); err != nil {
		return err
	}

	var resultSecParams *UsmSecurityParameters

	if resultSecParams, err = castUsmSecParams(result.SecurityParameters); err != nil {
		return err
	}

	if x.MsgFlags&AuthNoPriv > 0 {
		if !isAuthentic(packet, resultSecParams.AuthenticationParameters,
			secParameters.AuthenticationProtocol,
			secParameters.AuthenticationPassphrase,
			secParameters.AuthoritativeEngineID) {
			return fmt.Errorf("Incoming packet is not authentic, discarding")
		}
	}

	return nil
}

// determine whether a message is authentic
func isAuthentic(msg []byte, authParams string, authProtocol SnmpV3AuthProtocol, authPassphrase string, authEngineID string) bool {
	var secretKey = genlocalkey(authProtocol,
		authPassphrase,
		authEngineID)

	var extkey [64]byte

	copy(extkey[:], secretKey)

	var k1, k2 [64]byte

	for i := 0; i < 64; i++ {
		k1[i] = extkey[i] ^ 0x36
		k2[i] = extkey[i] ^ 0x5c
	}

	var h, h2 hash.Hash

	switch authProtocol {
	default:
		h = md5.New()
		h2 = md5.New()
	case SHA:
		h = sha1.New()
		h2 = sha1.New()
	}

	h.Write(k1[:])
	h.Write(msg)
	d1 := h.Sum(nil)
	h2.Write(k2[:])
	h2.Write(d1)

	result := h2.Sum(nil)[:12]
	for k, v := range []byte(authParams) {
		if result[k] != v {
			return false
		}
	}
	return true
}

// MD5 HMAC key calculation algorithm
func md5HMAC(password string, engineID string) []byte {
	comp := md5.New()
	var pi int // password index
	for i := 0; i < 1048576; i += 64 {
		var chunk []byte
		for e := 0; e < 64; e++ {
			chunk = append(chunk, password[pi%len(password)])
			pi++
		}
		comp.Write(chunk)
	}
	compressed := comp.Sum(nil)
	local := md5.New()
	local.Write(compressed)
	local.Write([]byte(engineID))
	local.Write(compressed)
	final := local.Sum(nil)
	return final
}

// SHA HMAC key calculation algorithm
func shaHMAC(password string, engineID string) []byte {
	hash := sha1.New()
	var pi int // password index
	for i := 0; i < 1048576; i += 64 {
		var chunk []byte
		for e := 0; e < 64; e++ {
			chunk = append(chunk, password[pi%len(password)])
			pi++
		}
		hash.Write(chunk)
	}
	hashed := hash.Sum(nil)
	local := sha1.New()
	local.Write(hashed)
	local.Write([]byte(engineID))
	local.Write(hashed)
	final := local.Sum(nil)
	return final
}

func genlocalkey(authProtocol SnmpV3AuthProtocol, passphrase string, engineID string) []byte {
	var secretKey []byte
	switch authProtocol {
	default:
		secretKey = md5HMAC(passphrase, engineID)
	case SHA:
		secretKey = shaHMAC(passphrase, engineID)
	}
	return secretKey
}

func castUsmSecParams(secParams SnmpV3SecurityParameters) (*UsmSecurityParameters, error) {
	s, ok := secParams.(*UsmSecurityParameters)
	if !ok || s == nil {
		return nil, fmt.Errorf("SecurityParameters is not of type *UsmSecurityParameters")
	}
	return s, nil
}

// http://tools.ietf.org/html/rfc2574#section-8.1.1.1
// localDESSalt needs to be incremented on every packet.
func (x *GoSNMP) usmAllocateNewSalt() (interface{}, error) {
	var s *UsmSecurityParameters
	var err error

	if s, err = castUsmSecParams(x.SecurityParameters); err != nil {
		return nil, err
	}
	var newSalt interface{}
	switch s.PrivacyProtocol {
	case AES:
		newSalt = atomic.AddUint64(&(s.localAESSalt), 1)
	default:
		newSalt = atomic.AddUint32(&(s.localDESSalt), 1)
	}
	return newSalt, nil
}

func (packet *SnmpPacket) setUsmSalt(newSalt interface{}) error {
	var s *UsmSecurityParameters
	var err error

	if s, err = castUsmSecParams(packet.SecurityParameters); err != nil {
		return err
	}

	switch s.PrivacyProtocol {
	case AES:
		aesSalt, ok := newSalt.(uint64)
		if !ok {
			return fmt.Errorf("salt provided to setUsmSalt is not the correct type for the AES privacy protocol")
		}
		var salt = make([]byte, 8)
		binary.BigEndian.PutUint64(salt, aesSalt)
		s.PrivacyParameters = salt
	default:
		desSalt, ok := newSalt.(uint32)
		if !ok {
			return fmt.Errorf("salt provided to setUsmSalt is not the correct type for the DES privacy protocol")
		}
		var salt = make([]byte, 8)
		binary.BigEndian.PutUint32(salt, s.AuthoritativeEngineBoots)
		binary.BigEndian.PutUint32(salt[4:], desSalt)
		s.PrivacyParameters = salt
	}
	return nil
}

func (x *GoSNMP) saltNewPacket(packetOut *SnmpPacket) error {

	if x.MsgFlags&AuthPriv > AuthNoPriv && x.SecurityModel == UserSecurityModel {
		// http://tools.ietf.org/html/rfc2574#section-8.1.1.1
		// localDESSalt needs to be incremented on every packet.
		newSalt, err := x.usmAllocateNewSalt()
		if err != nil {
			return err
		}
		if packetOut.Version == Version3 && packetOut.SecurityModel == UserSecurityModel && packetOut.MsgFlags&AuthPriv > AuthNoPriv {

			err = packetOut.setUsmSalt(newSalt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// http://tools.ietf.org/html/rfc2574#section-2.2.3 This code does not
// check if the last message received was more than 150 seconds ago The
// snmpds that this code was tested on emit an 'out of time window'
// error with the new time and this code will retransmit when that is
// received.
func (x *GoSNMP) negotiateInitialSecurityParameters(packetOut *SnmpPacket, wait bool) error {
	if x.Version != Version3 || packetOut.Version != Version3 {
		return fmt.Errorf("negotiateInitialSecurityParameters called with non Version3 connection or packet")
	}

	if x.SecurityModel != packetOut.SecurityModel {
		return fmt.Errorf("connection security model does not match security model defined in packet")
	}

	if packetOut.SecurityModel == UserSecurityModel {
		var secParams *UsmSecurityParameters
		var err error

		if secParams, err = castUsmSecParams(packetOut.SecurityParameters); err != nil {
			return err
		}

		if secParams.AuthoritativeEngineID == "" {
			var emptyPdus []SnmpPDU

			// send blank packet to discover authoriative engine ID/boots/time
			blankPacket := &SnmpPacket{
				Version:            Version3,
				MsgFlags:           Reportable | NoAuthNoPriv,
				SecurityModel:      UserSecurityModel,
				SecurityParameters: &UsmSecurityParameters{},
				PDUType:            GetRequest,
				Logger:             x.Logger,
				Variables:          emptyPdus,
			}
			result, err := x.sendOneRequest(blankPacket, wait)

			if err != nil {
				return err
			}

			err = x.storeSecurityParameters(result)
			if err != nil {
				return err
			}

			err = x.updatePktSecurityParameters(packetOut)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// save the connection security parameters after a request/response
func (x *GoSNMP) storeSecurityParameters(result *SnmpPacket) error {

	if x.Version != Version3 || result.Version != Version3 {
		return fmt.Errorf("storeParameters called with non Version3 connection or packet")
	}

	if x.SecurityModel != result.SecurityModel {
		return fmt.Errorf("connection security model does not match security model extracted from packet")
	}

	if result.SecurityModel == UserSecurityModel {
		var newSecParams *UsmSecurityParameters
		var err error

		if newSecParams, err = castUsmSecParams(result.SecurityParameters); err != nil {
			return err
		}
		connSecParams, _ := x.SecurityParameters.(*UsmSecurityParameters)
		if connSecParams != nil {
			connSecParams.AuthoritativeEngineID = newSecParams.AuthoritativeEngineID
			connSecParams.AuthoritativeEngineBoots = newSecParams.AuthoritativeEngineBoots
			connSecParams.AuthoritativeEngineTime = newSecParams.AuthoritativeEngineTime
		}
		if x.ContextEngineID == "" {
			x.ContextEngineID = newSecParams.AuthoritativeEngineID
		}
	}

	return nil

}

// update packet security parameters to match connection security parameters
func (x *GoSNMP) updatePktSecurityParameters(packetOut *SnmpPacket) error {
	if x.Version != Version3 || packetOut.Version != Version3 {
		return fmt.Errorf("updatePktSecurityParameters called with non Version3 connection or packet")
	}

	if x.SecurityModel != packetOut.SecurityModel {
		return fmt.Errorf("connection security model does not match security model extracted from packet")
	}

	if x.SecurityModel == UserSecurityModel {
		var c *UsmSecurityParameters
		var err error
		if c, err = castUsmSecParams(x.SecurityParameters); err != nil {
			return err
		}

		var s *UsmSecurityParameters
		if s, err = castUsmSecParams(packetOut.SecurityParameters); err != nil {
			return err
		}

		s.AuthoritativeEngineID = c.AuthoritativeEngineID
		s.AuthoritativeEngineBoots = c.AuthoritativeEngineBoots
		s.AuthoritativeEngineTime = c.AuthoritativeEngineTime

	}

	if packetOut.ContextEngineID == "" {
		packetOut.ContextEngineID = x.ContextEngineID
	}

	return nil
}

func (packet *SnmpPacket) marshalV3(buf *bytes.Buffer) (*bytes.Buffer, uint32, error) {

	emptyBuffer := new(bytes.Buffer) // used when returning errors
	var authParamStart uint32

	header, err := packet.marshalV3Header()
	if err != nil {
		return emptyBuffer, 0, err
	}
	buf.Write([]byte{byte(Sequence), byte(len(header))})
	buf.Write(header)

	var securityParameters []byte
	if packet.SecurityModel == UserSecurityModel {
		securityParameters, authParamStart, err = packet.marshalV3UsmSecurityParameters()
		if err != nil {
			return emptyBuffer, 0, err
		}
	}

	buf.Write([]byte{byte(OctetString)})
	secParamLen, err := marshalLength(len(securityParameters))
	if err != nil {
		return emptyBuffer, 0, err
	}
	buf.Write(secParamLen)
	authParamStart += uint32(buf.Len())
	buf.Write(securityParameters)

	scopedPdu, err := packet.marshalV3ScopedPDU()
	if err != nil {
		return emptyBuffer, 0, err
	}
	buf.Write(scopedPdu)
	return buf, authParamStart, nil
}

// marshal a snmp version 3 packet header
func (packet *SnmpPacket) marshalV3Header() ([]byte, error) {
	buf := new(bytes.Buffer)

	// msg id
	buf.Write([]byte{byte(Integer), 4})
	err := binary.Write(buf, binary.BigEndian, packet.MsgID)
	if err != nil {
		return nil, err
	}

	// maximum response msg size
	maxmsgsize := marshalUvarInt(rxBufSize)
	buf.Write([]byte{byte(Integer), byte(len(maxmsgsize))})
	buf.Write(maxmsgsize)

	// msg flags
	buf.Write([]byte{byte(OctetString), 1, byte(packet.MsgFlags)})

	// msg security model
	buf.Write([]byte{byte(Integer), 1, byte(packet.SecurityModel)})

	return buf.Bytes(), nil
}

// marshal a snmp version 3 security parameters field for the User Security Model
func (packet *SnmpPacket) marshalV3UsmSecurityParameters() ([]byte, uint32, error) {
	var buf bytes.Buffer
	var authParamStart uint32

	var secParams *UsmSecurityParameters
	var err error

	if secParams, err = castUsmSecParams(packet.SecurityParameters); err != nil {
		return nil, 0, err
	}

	// msgAuthoritativeEngineID
	buf.Write([]byte{byte(OctetString), byte(len(secParams.AuthoritativeEngineID))})
	buf.WriteString(secParams.AuthoritativeEngineID)

	// msgAuthoritativeEngineBoots
	msgAuthoritativeEngineBoots := marshalUvarInt(secParams.AuthoritativeEngineBoots)
	buf.Write([]byte{byte(Integer), byte(len(msgAuthoritativeEngineBoots))})
	buf.Write(msgAuthoritativeEngineBoots)

	// msgAuthoritativeEngineTime
	msgAuthoritativeEngineTime := marshalUvarInt(secParams.AuthoritativeEngineTime)
	buf.Write([]byte{byte(Integer), byte(len(msgAuthoritativeEngineTime))})
	buf.Write(msgAuthoritativeEngineTime)

	// msgUserName
	buf.Write([]byte{byte(OctetString), byte(len(secParams.UserName))})
	buf.WriteString(secParams.UserName)

	authParamStart = uint32(buf.Len() + 2) // +2 indicates PDUType + Length
	// msgAuthenticationParameters
	if packet.MsgFlags&AuthNoPriv > 0 {
		buf.Write([]byte{byte(OctetString), 12,
			0, 0, 0, 0,
			0, 0, 0, 0,
			0, 0, 0, 0})
	} else {
		buf.Write([]byte{byte(OctetString), 0})
	}
	// msgPrivacyParameters
	if packet.MsgFlags&AuthPriv > AuthNoPriv {
		privlen, err := marshalLength(len(secParams.PrivacyParameters))
		if err != nil {
			return nil, 0, err
		}
		buf.Write([]byte{byte(OctetString)})
		buf.Write(privlen)
		buf.Write(secParams.PrivacyParameters)
	} else {
		buf.Write([]byte{byte(OctetString), 0})
	}

	// wrap security parameters in a sequence
	paramLen, err := marshalLength(buf.Len())
	if err != nil {
		return nil, 0, err
	}
	tmpseq := append([]byte{byte(Sequence)}, paramLen...)
	authParamStart += uint32(len(tmpseq))
	tmpseq = append(tmpseq, buf.Bytes()...)

	return tmpseq, authParamStart, nil
}

// marshal and encrypt (if necessary) a snmp version 3 Scoped PDU
func (packet *SnmpPacket) marshalV3ScopedPDU() ([]byte, error) {
	var b []byte

	scopedPdu, err := packet.prepareV3ScopedPDU()
	if err != nil {
		return nil, err
	}
	pduLen, err := marshalLength(len(scopedPdu))
	if err != nil {
		return nil, err
	}
	b = append([]byte{byte(Sequence)}, pduLen...)
	scopedPdu = append(b, scopedPdu...)
	if packet.MsgFlags&AuthPriv > AuthNoPriv && packet.SecurityModel == UserSecurityModel {
		var secParams *UsmSecurityParameters

		if secParams, err = castUsmSecParams(packet.SecurityParameters); err != nil {
			return nil, err
		}

		var privkey = genlocalkey(secParams.AuthenticationProtocol,
			secParams.PrivacyPassphrase,
			secParams.AuthoritativeEngineID)
		switch secParams.PrivacyProtocol {
		case AES:
			var iv [16]byte
			binary.BigEndian.PutUint32(iv[:], secParams.AuthoritativeEngineBoots)
			binary.BigEndian.PutUint32(iv[4:], secParams.AuthoritativeEngineTime)
			copy(iv[8:], secParams.PrivacyParameters)

			block, err := aes.NewCipher(privkey[:16])
			if err != nil {
				return nil, err
			}
			stream := cipher.NewCFBEncrypter(block, iv[:])
			ciphertext := make([]byte, len(scopedPdu))
			stream.XORKeyStream(ciphertext, scopedPdu)
			pduLen, err := marshalLength(len(ciphertext))
			if err != nil {
				return nil, err
			}
			b = append([]byte{byte(OctetString)}, pduLen...)
			scopedPdu = append(b, ciphertext...)
		default:
			preiv := privkey[8:]
			var iv [8]byte
			for i := 0; i < len(iv); i++ {
				iv[i] = preiv[i] ^ secParams.PrivacyParameters[i]
			}
			block, err := des.NewCipher(privkey[:8])
			if err != nil {
				return nil, err
			}
			mode := cipher.NewCBCEncrypter(block, iv[:])

			pad := make([]byte, des.BlockSize-len(scopedPdu)%des.BlockSize)
			scopedPdu = append(scopedPdu, pad...)

			ciphertext := make([]byte, len(scopedPdu))
			mode.CryptBlocks(ciphertext, scopedPdu)
			pduLen, err := marshalLength(len(ciphertext))
			if err != nil {
				return nil, err
			}
			b = append([]byte{byte(OctetString)}, pduLen...)
			scopedPdu = append(b, ciphertext...)
		}

	}

	return scopedPdu, nil
}

// prepare the plain text of a snmp version 3 Scoped PDU
func (packet *SnmpPacket) prepareV3ScopedPDU() ([]byte, error) {
	var buf bytes.Buffer

	//ContextEngineID
	idlen, err := marshalLength(len(packet.ContextEngineID))
	if err != nil {
		return nil, err
	}
	buf.Write(append([]byte{byte(OctetString)}, idlen...))
	buf.WriteString(packet.ContextEngineID)

	//ContextName
	namelen, err := marshalLength(len(packet.ContextName))
	if err != nil {
		return nil, err
	}
	buf.Write(append([]byte{byte(OctetString)}, namelen...))
	buf.WriteString(packet.ContextName)

	data, err := packet.marshalPDU()
	if err != nil {
		return nil, err
	}
	buf.Write(data)
	return buf.Bytes(), nil
}

func (x *GoSNMP) unmarshalV3Header(packet []byte,
	cursor int,
	response *SnmpPacket) (int, error) {

	if PDUType(packet[cursor]) != Sequence {
		return 0, fmt.Errorf("Invalid SNMPV3 Header\n")
	}

	_, cursorTmp := parseLength(packet[cursor:])
	cursor += cursorTmp

	rawMsgID, count, err := parseRawField(packet[cursor:], "msgID")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 message ID: %s", err.Error())
	}
	cursor += count
	if MsgID, ok := rawMsgID.(int); ok {
		response.MsgID = uint32(MsgID)
		x.logPrintf("Parsed message ID %d", MsgID)
	}
	// discard msg max size
	_, count, err = parseRawField(packet[cursor:], "maxMsgSize")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 maxMsgSize: %s", err.Error())
	}
	cursor += count
	// discard msg max size

	rawMsgFlags, count, err := parseRawField(packet[cursor:], "msgFlags")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 msgFlags: %s", err.Error())
	}
	cursor += count
	if MsgFlags, ok := rawMsgFlags.(string); ok {
		response.MsgFlags = SnmpV3MsgFlags(MsgFlags[0])
		x.logPrintf("parsed msg flags %s", MsgFlags)
	}

	rawSecModel, count, err := parseRawField(packet[cursor:], "msgSecurityModel")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 msgSecModel: %s", err.Error())
	}
	cursor += count
	if SecModel, ok := rawSecModel.(int); ok {
		response.SecurityModel = SnmpV3SecurityModel(SecModel)
		x.logPrintf("Parsed security model %d", SecModel)
	}

	if PDUType(packet[cursor]) != OctetString {
		return 0, fmt.Errorf("Invalid SNMPV3 Security Parameters\n")
	}
	_, cursorTmp = parseLength(packet[cursor:])
	cursor += cursorTmp
	if response.SecurityModel == UserSecurityModel {
		cursor, err = x.unmarshalUsmSecurityParameters(packet, cursor, response)
		if err != nil {
			return 0, err
		}
	}
	return cursor, nil
}

func (x *GoSNMP) decryptPacket(packet []byte, cursor int, response *SnmpPacket) ([]byte, int, error) {
	switch PDUType(packet[cursor]) {
	case OctetString:
		// pdu is encrypted
		_, cursorTmp := parseLength(packet[cursor:])
		cursorTmp += cursor

		if response.SecurityModel == UserSecurityModel {
			var secParams *UsmSecurityParameters
			var err error
			if secParams, err = castUsmSecParams(response.SecurityParameters); err != nil {
				return nil, 0, err
			}

			var privkey = genlocalkey(secParams.AuthenticationProtocol,
				secParams.PrivacyPassphrase,
				secParams.AuthoritativeEngineID)
			switch secParams.PrivacyProtocol {
			case AES:
				var iv [16]byte
				binary.BigEndian.PutUint32(iv[:], secParams.AuthoritativeEngineBoots)
				binary.BigEndian.PutUint32(iv[4:], secParams.AuthoritativeEngineTime)
				copy(iv[8:], secParams.PrivacyParameters)

				block, err := aes.NewCipher(privkey[:16])
				if err != nil {
					return nil, 0, err
				}
				stream := cipher.NewCFBDecrypter(block, iv[:])
				plaintext := make([]byte, len(packet[cursorTmp:]))
				stream.XORKeyStream(plaintext, packet[cursorTmp:])
				copy(packet[cursor:], plaintext)
				packet = packet[:cursor+len(plaintext)]
			default:
				if len(packet[cursorTmp:])%des.BlockSize != 0 {
					return nil, 0, fmt.Errorf("Error decrypting ScopedPDU: not multiple of des block size.")
				}
				preiv := privkey[8:]
				var iv [8]byte
				for i := 0; i < len(iv); i++ {
					iv[i] = preiv[i] ^ secParams.PrivacyParameters[i]
				}
				block, err := des.NewCipher(privkey[:8])
				if err != nil {
					return nil, 0, err
				}
				mode := cipher.NewCBCDecrypter(block, iv[:])

				plaintext := make([]byte, len(packet[cursorTmp:]))
				mode.CryptBlocks(plaintext, packet[cursorTmp:])
				copy(packet[cursor:], plaintext)
				// truncate packet to remove extra space caused by the
				// octetstring/length header that was just replaced
				packet = packet[:cursor+len(plaintext)]
			}

		}
		fallthrough
	case Sequence:
		// pdu is plaintext
		tlength, cursorTmp := parseLength(packet[cursor:])
		// truncate padding that may have been included with
		// the encrypted PDU
		packet = packet[:cursor+tlength]
		cursor += cursorTmp
		rawContextEngineID, count, err := parseRawField(packet[cursor:], "contextEngineID")
		if err != nil {
			return nil, 0, fmt.Errorf("Error parsing SNMPV3 contextEngineID: %s", err.Error())
		}
		cursor += count
		if contextEngineID, ok := rawContextEngineID.(string); ok {
			response.ContextEngineID = contextEngineID
			x.logPrintf("Parsed contextEngineID %s", contextEngineID)
		}
		rawContextName, count, err := parseRawField(packet[cursor:], "contextName")
		if err != nil {
			return nil, 0, fmt.Errorf("Error parsing SNMPV3 contextName: %s", err.Error())
		}
		cursor += count
		if contextName, ok := rawContextName.(string); ok {
			response.ContextName = contextName
			x.logPrintf("Parsed contextName %s", contextName)
		}

	default:
		return nil, 0, fmt.Errorf("Error parsing SNMPV3 scoped PDU\n")
	}
	return packet, cursor, nil
}

func (x *GoSNMP) unmarshalUsmSecurityParameters(packet []byte,
	cursor int,
	response *SnmpPacket) (int, error) {

	var secParameters *UsmSecurityParameters
	var err error

	if secParameters, err = castUsmSecParams(response.SecurityParameters); err != nil {
		return 0, err
	}

	if PDUType(packet[cursor]) != Sequence {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model parameters\n")
	}
	_, cursorTmp := parseLength(packet[cursor:])
	cursor += cursorTmp

	rawMsgAuthoritativeEngineID, count, err := parseRawField(packet[cursor:], "msgAuthoritativeEngineID")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgAuthoritativeEngineID: %s", err.Error())
	}
	cursor += count
	if AuthoritativeEngineID, ok := rawMsgAuthoritativeEngineID.(string); ok {
		secParameters.AuthoritativeEngineID = AuthoritativeEngineID
		x.logPrintf("Parsed authoritativeEngineID %s", AuthoritativeEngineID)
	}

	rawMsgAuthoritativeEngineBoots, count, err := parseRawField(packet[cursor:], "msgAuthoritativeEngineBoots")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgAuthoritativeEngineBoots: %s", err.Error())
	}
	cursor += count
	if AuthoritativeEngineBoots, ok := rawMsgAuthoritativeEngineBoots.(int); ok {
		secParameters.AuthoritativeEngineBoots = uint32(AuthoritativeEngineBoots)
		x.logPrintf("Parsed authoritativeEngineBoots %d", AuthoritativeEngineBoots)
	}

	rawMsgAuthoritativeEngineTime, count, err := parseRawField(packet[cursor:], "msgAuthoritativeEngineTime")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgAuthoritativeEngineTime: %s", err.Error())
	}
	cursor += count
	if AuthoritativeEngineTime, ok := rawMsgAuthoritativeEngineTime.(int); ok {
		secParameters.AuthoritativeEngineTime = uint32(AuthoritativeEngineTime)
		x.logPrintf("Parsed authoritativeEngineTime %d", AuthoritativeEngineTime)
	}

	rawMsgUserName, count, err := parseRawField(packet[cursor:], "msgUserName")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgUserName: %s", err.Error())
	}
	cursor += count
	if msgUserName, ok := rawMsgUserName.(string); ok {
		secParameters.UserName = msgUserName
		x.logPrintf("Parsed userName %s", msgUserName)
	}

	rawMsgAuthParameters, count, err := parseRawField(packet[cursor:], "msgAuthenticationParameters")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgAuthenticationParameters: %s", err.Error())
	}
	if msgAuthenticationParameters, ok := rawMsgAuthParameters.(string); ok {
		secParameters.AuthenticationParameters = msgAuthenticationParameters
		x.logPrintf("Parsed authenticationParameters %s", msgAuthenticationParameters)
	}
	// blank msgAuthenticationParameters to prepare for authentication check later
	if response.MsgFlags&AuthNoPriv > 0 {
		blank := make([]byte, 12)
		copy(packet[cursor+2:cursor+14], blank)
	}
	cursor += count

	rawMsgPrivacyParameters, count, err := parseRawField(packet[cursor:], "msgPrivacyParameters")
	if err != nil {
		return 0, fmt.Errorf("Error parsing SNMPV3 User Security Model msgPrivacyParameters: %s", err.Error())
	}
	cursor += count
	if msgPrivacyParameters, ok := rawMsgPrivacyParameters.(string); ok {
		secParameters.PrivacyParameters = []byte(msgPrivacyParameters)
		x.logPrintf("Parsed privacyParameters %s", msgPrivacyParameters)
	}

	return cursor, nil
}