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

	idFieldIndex        int
	createdAtFieldIndex int
	updatedAtFieldIndex int

	numField          int
	isSliceOfPointers bool
	isStructPointer   bool
	isStruct          bool
	isValidSlice      bool
	columns           []reflect.StructField
}

var errInvalidType = fmt.Errorf("destination must be a struct, or a slice of structs")

func newModelType(t any) (*modelType, error) {
	baseType := reflect.TypeOf(t)
	elemType := baseType

	for elemType.Kind() == reflect.Pointer || elemType.Kind() == reflect.Slice {
		elemType = elemType.Elem()
	}

	if elemType.Kind() != reflect.Struct {
		return nil, errInvalidType
	}

	var tableName string
	instance := reflect.New(elemType)
	if instance.Type().Implements(reflect.TypeOf((*TableNamer)(nil)).Elem()) {
		tableNamer := instance.Interface().(TableNamer)
		tableName = tableNamer.TableName()
	} else {
		tableName = snake_case(elemType.Name())
	}

	model := &modelType{
		tableName:         tableName,
		elemType:          elemType,
		baseType:          baseType,
		numField:          elemType.NumField(),
		isSliceOfPointers: determineIsSliceOfPointers(baseType),
		isStructPointer:   determineIsStructPointer(baseType),
		isStruct:          determineIsStruct(baseType),
		isValidSlice:      determineIsValidSlice(baseType, elemType),
		columns:           make([]reflect.StructField, 0, elemType.NumField()),

		// indexes will get replaced with real values if found in the `findColumns` call below
		idFieldIndex:        -1,
		createdAtFieldIndex: -1,
		updatedAtFieldIndex: -1,
	}

	findColumns(model, elemType)

	return model, nil
}

func findColumns(m *modelType, elem reflect.Type) {
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		if !field.IsExported() {
			continue
		}

		// Skip fields with db:"-", since they should be ignored
		if field.Tag.Get("db") == "-" {
			continue
		}

		if (field.Tag.Get("db") == "" && (field.Name == "ID")) || field.Tag.Get("db") == "id" {
			m.idFieldIndex = i
		}

		if (field.Tag.Get("db") == "" && (field.Name == "CreatedAt")) || field.Tag.Get("db") == "created_at" {
			m.createdAtFieldIndex = i
		}

		if (field.Tag.Get("db") == "" && (field.Name == "UpdatedAt")) || field.Tag.Get("db") == "updated_at" {
			m.updatedAtFieldIndex = i
		}

		m.columns = append(m.columns, field)
	}
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

func determineIsStruct(baseType reflect.Type) bool {
	return baseType.Kind() == reflect.Struct
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
