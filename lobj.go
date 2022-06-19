package sqlq

import (
	"bytes"
	"io"

	"github.com/jackc/pgx/v4"
	"github.com/n-r-w/nerr"
)

// SaveLargeObject - write Large Object to the database. If oid == 0 then creates a new object.
// Returns the id of the created or updated object
func SaveLargeObject(tx *Tx, oid uint32, data []byte) (uint32, error) {
	lobj := tx.tx.LargeObjects()
	var obj *pgx.LargeObject
	var err error
	if oid > 0 {
		obj, err = lobj.Open(tx.ctx, oid, pgx.LargeObjectModeWrite)
	} else {
		oid, err = lobj.Create(tx.ctx, oid)
		if err != nil {
			return 0, nerr.New(err)
		}
		obj, err = lobj.Open(tx.ctx, oid, pgx.LargeObjectModeWrite)
	}
	if err != nil {
		return 0, nerr.New(err)
	}

	_, err = obj.Write(data)
	if err != nil {
		return 0, nerr.New(err)
	}

	return oid, nil
}

// LoadLargeObject - read Large Object from the database.
func LoadLargeObject(tx *Tx, oid uint32) ([]byte, error) {
	var err error

	lobj := tx.tx.LargeObjects()
	obj, err := lobj.Open(tx.ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		return []byte{}, nerr.New(err)
	}

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, obj)
	if err != nil {
		return []byte{}, nerr.New(err)
	}

	return buf.Bytes(), nil
}

// RemoveLargeObject - remove Large Object from the database.
func RemoveLargeObject(tx *Tx, oid uint32) error {
	lobj := tx.tx.LargeObjects()
	return nerr.New(lobj.Unlink(tx.ctx, oid))
}
