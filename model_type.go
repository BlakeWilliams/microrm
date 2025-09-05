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

	numField          int
	isSliceOfPointers bool
	isStructPointer   bool
	isValidSlice      bool
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
		tableName:         tableName,
		elemType:          elemType,
		baseType:          baseType,
		numField:          elemType.NumField(),
		isSliceOfPointers: determineIsSliceOfPointers(baseType),
		isStructPointer:   determineIsStructPointer(baseType),
		isValidSlice:      determineIsValidSlice(baseType, elemType),
	}, nil
}

func (m *modelType) FieldType(i int) reflect.StructField {
	return m.elemType.Field(i)
}

func (m *modelType) NewElem() reflect.Value {
	return reflect.New(m.elemType).Elem()
}

func determineIsSliceOfPointers(baseType reflect.Type) bool {
	t := baseType
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		return t.Elem().Kind() == reflect.Pointer
	}
	return false
}

func determineIsStructPointer(baseType reflect.Type) bool {
	return baseType.Kind() == reflect.Pointer && baseType.Elem().Kind() == reflect.Struct
}

func determineIsValidSlice(baseType, elemType reflect.Type) bool {
	t := baseType
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Slice && (elemType.Kind() == reflect.Struct ||
		elemType.Kind() == reflect.Pointer &&
			elemType.Elem().Kind() == reflect.Struct)
}
