package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"time"
)

const (
	AuxFieldRedisVer  = "redis-ver"
	AuxFieldRedisBits = "redis-bits"
	AuxFieldCtime     = "ctime"
	AuxFieldUsedMem   = "used-mem"
)

const (
	OPCodeEOF          = 0xFF
	OPCodeSELECTDB     = 0xFE
	OPCodeEXPIRETIME   = 0xFD
	OPCodeEXPIRETIMEMS = 0xFC
	OPCodeRESIZEDB     = 0xFB
	OPCodeAUX          = 0xFA
)

type RDB struct {
	// Magic
	MagicString [5]byte // 5 bytes SHOULD BE "REDIS"
	RDBVerNum   [4]byte // 4 bytes

	// Auxiliary field
	AuxField  map[string]string
	Databases []*Database
}

type Database struct {
	ID       int
	ResizeDB struct {
		HashTableSize   int
		ExpireHashTable int
	}
	Fields map[string]Field
}

func (db *Database) Set(key string, value string) {
	db.Fields[key] = Field{
		Key:   key,
		Type:  FieldTypeString,
		Value: StringValue(value),
	}
}

func (db *Database) Unset(key string) {
	delete(db.Fields, key)
}

func (db *Database) UnsetAfter(duration time.Duration, key string) {
	go func() {
		time.AfterFunc(duration, func() {
			db.Unset(key)
		})
	}()
}

func (db *Database) Get(key string) (string, bool) {
	field, ok := db.Fields[key]
	if !ok {
		return "", false
	}

	if field.Type != FieldTypeString {
		return "", false
	}

	return string(field.Value.(StringValue)), true
}

type FieldType byte

const (
	FieldTypeString FieldType = 0
)

type Field struct {
	Key         string
	ExpiredTime time.Time
	Type        FieldType
	Value       any
}

type StringValue string

func ParseFile(file io.Reader) (RDB, error) {
	r := bufio.NewReader(file)

	var rdb RDB
	rdb.AuxField = map[string]string{}

	_, err := r.Read(rdb.MagicString[:])
	if err != nil {
		return RDB{}, err
	}

	_, err = r.Read(rdb.RDBVerNum[:])
	if err != nil {
		return RDB{}, err
	}

	var curDBID int
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			break
		}

		if err != nil {
			return RDB{}, err
		}

		if b == OPCodeEOF {
			break
		}

		switch b {
		case OPCodeAUX:
			key, value, err := parseAux(r)
			if err != nil {
				return RDB{}, err
			}

			if isValidAuxKey(key) {
				rdb.AuxField[key] = value
			}

			continue
		case OPCodeSELECTDB:
			db := &Database{}
			dbID, err := DecodeLength(r)
			if err != nil {
				return RDB{}, err
			}

			db.ID = dbID
			db.Fields = map[string]Field{}
			curDBID = dbID

			rdb.Databases = append(rdb.Databases, db)
			continue
		case OPCodeRESIZEDB:
			hashTableSize, err := DecodeLength(r)
			if err != nil {
				return RDB{}, err
			}
			rdb.Databases[curDBID].ResizeDB.HashTableSize = hashTableSize

			expireHashTableSize, err := DecodeLength(r)
			if err != nil {
				return RDB{}, err
			}
			rdb.Databases[curDBID].ResizeDB.ExpireHashTable = expireHashTableSize
			continue
		default:
			var f Field
			switch b {
			case OPCodeEXPIRETIME:
				var data uint32
				if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
					return RDB{}, err
				}
				f.ExpiredTime = time.Unix(int64(data), 0)
				b, err := r.ReadByte()
				if err != nil {
					return RDB{}, err
				}

				f.Type = FieldType(b)
			case OPCodeEXPIRETIMEMS:
				var data uint64
				if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
					return RDB{}, err
				}

				f.ExpiredTime = time.UnixMilli(int64(data))
				b, err := r.ReadByte()
				if err != nil {
					return RDB{}, err
				}

				f.Type = FieldType(b)
			default:
				f.Type = FieldType(b)
			}

			key, err := DecodeString(r)
			if err != nil {
				return RDB{}, err
			}

			f.Key = key

			switch f.Type {
			case FieldTypeString:
				val, err := DecodeString(r)
				if err != nil {
					return RDB{}, err
				}
				f.Value = StringValue(val)
			}

			rdb.Databases[curDBID].Fields[key] = f

			if f.ExpiredTime != (time.Time{}) {
				rdb.Databases[curDBID].UnsetAfter(time.Until(f.ExpiredTime), key)
			}
		}
	}

	return rdb, nil
}

func parseAux(r *bufio.Reader) (string, string, error) {
	var kv [2]string

	for i := 0; i < len(kv); i++ {
		str, err := DecodeString(r)
		if err != nil {
			return "", "", err
		}

		kv[i] = str
	}

	return kv[0], kv[1], nil
}

func isValidAuxKey(key string) bool {
	switch key {
	case AuxFieldRedisVer,
		AuxFieldRedisBits,
		AuxFieldCtime,
		AuxFieldUsedMem:
		return true
	}

	return false
}
