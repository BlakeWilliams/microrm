package microrm

import (
	"fmt"
	"reflect"
)

type modelType struct {
	tableName string
	// elemType is the type of the struct, e.g. User
	elemType reflect.Type

	// baseType is the type passed directly to DB methods, e.g. *[]User or []*User
	baseType reflect.Type

	value reflect.Value
}

var errInvalidType = fmt.Errorf("destination must be a struct, or a slice of structs")

func newModelType(t any, tableMap map[string]string) (*modelType, error) {
	baseType := reflect.TypeOf(t)
	elemType := baseType

	for elemType.Kind() == reflect.Pointer || elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return nil, errInvalidType
	}

	var tableName string
	if name, ok := tableMap[elemType.Name()]; ok {
		tableName = name
	} else {
		tableName = snake_case(elemType.Name())
	}

	return &modelType{
		tableName: tableName,
		elemType:  elemType,
		baseType:  baseType,
		value:     reflect.ValueOf(t),
	}, nil
}

func (m *modelType) NumField() int {
	return m.elemType.NumField()
}

func (m *modelType) FieldType(i int) reflect.StructField {
	return m.elemType.Field(i)
}

func (m *modelType) FieldValue(i int) reflect.Value {
	return m.SelfElem().Field(i)
}

func (m *modelType) SelfElem() reflect.Value {
	self := m.value
	if self.Kind() == reflect.Pointer {
		return self.Elem()
	}

	return self
}

func (m *modelType) IsSliceOfPointers() bool {
	t := m.baseType
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() == reflect.Slice {
		return t.Elem().Kind() == reflect.Pointer
	}

	return false
}

func (m *modelType) IsStructPointer() bool {
	return m.baseType.Kind() == reflect.Pointer && m.baseType.Elem().Kind() == reflect.Struct
}

func (m *modelType) NewElem() reflect.Value {
	return reflect.New(m.elemType).Elem()
}

func (m *modelType) IsValidSlice() bool {
	t := m.baseType
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	return t.Kind() == reflect.Slice && (m.elemType.Kind() == reflect.Struct ||
		m.elemType.Kind() == reflect.Pointer &&
			m.elemType.Elem().Kind() == reflect.Struct)
}
