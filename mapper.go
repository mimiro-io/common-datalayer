package common_datalayer

import (
	"fmt"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/pkg/errors"
	"reflect"
	"regexp"
	"strings"
)

type Mapper struct {
	logger                      Logger
	mappingConfig               *MappingConfig
	entityKeyMappings           map[string]*PropertyMapping
	itemKeyMappings             map[string]*PropertyMapping
	itemToEntityCustomTransform []func(item Item, entity *egdm.Entity) error
	entityToItemCustomTransform []func(entity *egdm.Entity, item Item) error
}

func NewMapper(logger Logger, mappingConfig *MappingConfig) *Mapper {
	mapper := &Mapper{
		logger:                      logger,
		mappingConfig:               mappingConfig,
		itemToEntityCustomTransform: make([]func(item Item, entity *egdm.Entity) error, 0),
		entityToItemCustomTransform: make([]func(entity *egdm.Entity, item Item) error, 0),
	}

	mapper.entityKeyMappings = make(map[string]*PropertyMapping)
	mapper.itemKeyMappings = make(map[string]*PropertyMapping)

	// enable fast lookup of the mappings
	for _, mapping := range mappingConfig.PropertyMappings {
		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty

		mapper.entityKeyMappings[entityPropertyName] = mapping
		mapper.itemKeyMappings[propertyName] = mapping
	}

	return mapper
}

func (mapper *Mapper) WithEntityToItemTransform(transform func(entity *egdm.Entity, item Item) error) *Mapper {
	mapper.entityToItemCustomTransform = append(mapper.entityToItemCustomTransform, transform)
	return mapper
}

func (mapper *Mapper) WithItemToEntityTransform(transform func(item Item, entity *egdm.Entity) error) *Mapper {
	mapper.itemToEntityCustomTransform = append(mapper.itemToEntityCustomTransform, transform)
	return mapper
}

func (mapper *Mapper) MapItemToEntity(item Item, entity *egdm.Entity) error {
	// apply constructions
	constructedProperties := make(map[string]any)

	for _, construction := range mapper.mappingConfig.Constructions {
		if construction.Operation == "concat" {
			if len(construction.Arguments) != 2 {
				return fmt.Errorf("concat operation requires two arguments")
			}
			concatedValue, err := concat(item.GetValue(construction.Arguments[0]), item.GetValue(construction.Arguments[1]))
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = concatedValue
		} else if construction.Operation == "split" {
			if len(construction.Arguments) != 2 {
				return fmt.Errorf("split operation requires two arguments")
			}
			spliter := construction.Arguments[1]
			splitValue, err := split(item.GetValue(construction.Arguments[0]), spliter)
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = splitValue
		} else if construction.Operation == "replace" {
			if len(construction.Arguments) != 3 {
				return fmt.Errorf("replace operation requires three arguments")
			}
			replacedValue, err := replace(item.GetValue(construction.Arguments[0]), construction.Arguments[1], construction.Arguments[2])
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = replacedValue
		} else if construction.Operation == "trim" {
			if len(construction.Arguments) != 1 {
				return fmt.Errorf("trim operation requires one argument")
			}
			trimmedValue, err := trim(item.GetValue(construction.Arguments[0]))
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = trimmedValue
		} else if construction.Operation == "tolower" {
			if len(construction.Arguments) != 1 {
				return fmt.Errorf("tolower operation requires one argument")
			}
			tolowerValue, err := tolower(item.GetValue(construction.Arguments[0]))
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = tolowerValue
		} else if construction.Operation == "toupper" {
			if len(construction.Arguments) != 1 {
				return fmt.Errorf("toupper operation requires one argument")
			}
			toupperValue, err := toupper(item.GetValue(construction.Arguments[0]))
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = toupperValue
		} else if construction.Operation == "regex" {
			if len(construction.Arguments) != 2 {
				return fmt.Errorf("regex operation requires two arguments")
			}
			regexValue, err := regex(item.GetValue(construction.Arguments[0]), construction.Arguments[1])
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = regexValue
		} else if construction.Operation == "slice" {
			if len(construction.Arguments) != 3 {
				return fmt.Errorf("slice operation requires three arguments")
			}
			start, err := intOfValue(construction.Arguments[1])
			if err != nil {
				return err
			}
			end, err := intOfValue(construction.Arguments[2])
			if err != nil {
				return err
			}
			slicedValue, err := slice(item.GetValue(construction.Arguments[0]), start, end)
			if err != nil {
				return err
			}
			constructedProperties[construction.PropertyName] = slicedValue
		} else if construction.Operation == "literal" {
			if len(construction.Arguments) != 1 {
				return fmt.Errorf("literal operation requires one argument")
			}
			constructedProperties[construction.PropertyName] = construction.Arguments[0]
		}
	}

	mappedProperties := make(map[string]bool)
	if mapper.mappingConfig.MapAllFromItem {
		for _, propertyName := range item.GetPropertyNames() {
			mappedProperties[propertyName] = false
		}
	}

	// apply mappings
	for _, mapping := range mapper.mappingConfig.PropertyMappings {
		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty
		if mapper.mappingConfig.MapAllFromItem {
			mappedProperties[propertyName] = true
		}

		if mapping.IsIdentity {
			idValue, err := stringOfValue(item.GetValue(propertyName))
			if err != nil {
				return errors.Wrap(err, "failed to convert identity value to string")
			}
			entity.ID = makeURL(mapping.UrlValuePattern, idValue)
		} else {
			if mapping.IsReference {
				// reference property
				referenceValue := item.GetValue(propertyName)
				if referenceValue != nil {
					referenceEntity := &egdm.Entity{ID: referenceValue.(string)}
					entity.Properties[entityPropertyName] = referenceEntity
				}
			} else {
				// regular property
				if _, ok := constructedProperties[propertyName]; ok {
					entity.Properties[entityPropertyName] = constructedProperties[propertyName]
				} else {
					entity.Properties[entityPropertyName] = item.GetValue(propertyName)
				}
			}
		}
	}

	// apply custom transforms

	return nil
}

func makeURL(urlPattern string, value string) string {
	return strings.ReplaceAll(urlPattern, "{value}", value)
}

func regex(v1 any, pattern string) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}

	// Compile the regular expression pattern
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	// Find the first match in the string
	match := re.FindString(s1)
	if match == "" {
		return "", fmt.Errorf("no match found for pattern: %s", pattern)
	}

	return match, nil
}

func slice(v1 any, start, end int) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	return s1[start:end], nil
}

func tolower(v1 any) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	return strings.ToLower(s1), nil
}

func toupper(v1 any) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	return strings.ToUpper(s1), nil
}

func trim(v1 any) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(s1), nil
}

func replace(v1 any, s2, s3 string) (string, error) {
	if v1 == nil {
		return "", fmt.Errorf("value is nil")
	}
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(s1, s2, s3), nil
}

func split(v1 any, split string) ([]string, error) {
	if v1 == nil {
		return nil, fmt.Errorf("value is nil")
	}

	v := reflect.ValueOf(v1)
	t := v.Type()

	var s1 string

	switch t.Kind() {
	case reflect.String:
		s1 = v.String()
	default:
		return nil, fmt.Errorf("split only works on string input %s", t.Kind())
	}
	return strings.Split(s1, split), nil
}

func concat(v1, v2 any) (string, error) {
	s1, err := stringOfValue(v1)
	if err != nil {
		return "", err
	}
	s2, err := stringOfValue(v2)
	if err != nil {
		return "", err
	}
	return s1 + s2, nil
}

func intOfValue(val interface{}) (int, error) {
	if val == nil {
		return 0, fmt.Errorf("value is nil")
	}

	v := reflect.ValueOf(val)
	t := v.Type()

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return int(v.Float()), nil
	default:
		return 0, fmt.Errorf("unsupported type %s", t.Kind())
	}
}

func stringOfValue(val interface{}) (string, error) {
	if val == nil {
		return "", fmt.Errorf("value is nil")
	}

	v := reflect.ValueOf(val)
	t := v.Type()

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", v.Float()), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Bool:
		return fmt.Sprintf("%t", v.Bool()), nil
	default:
		return "", fmt.Errorf("unsupported type %s", t.Kind())
	}
}

func (mapper *Mapper) MapEntityToItem(entity *egdm.Entity, item Item) error {
	return nil
}

type Item interface {
	// GetValue returns the value of the property with the given name
	GetValue(name string) any
	// SetValue sets the value of the property with the given name
	SetValue(name string, value any)
	// NativeItem returns the underlying native item
	NativeItem() any
	// Get Items Keys
	GetPropertyNames() []string
}

/*

type DataItem struct {
	raw map[string]any
}

func (d *DataItem) GetRaw() map[string]any {
	return d.raw
}

func (d *DataItem) PutRaw(raw map[string]any) {
	d.raw = raw
}

func (d *DataItem) GetValue(name string) any {
	return d.raw[name]
}

func (d *DataItem) SetValue(name string, value any) {
	d.raw[name] = value
}

type ItemIterator interface {
	Next() Item
}

type EntityIterator interface {
	Next() *egdm.Entity
	Token() string
	Close()
}

type MappingEntityIterator struct {
	mapper        ItemToEntityMapper
	rowIterator   ItemIterator
	customMapping func(mapping *PropertyMapping, item Item, entity egdm.Entity)
}

func (m MappingEntityIterator) Next() *egdm.Entity {
	rawItem := m.rowIterator.Next()
	if rawItem == nil {
		return nil
	}
	res := m.mapper.ItemToEntity(rawItem)
	return res
}

func (m MappingEntityIterator) Token() string {
	//TODO implement me
	panic("implement me")
}

func (m MappingEntityIterator) Close() {
	//TODO implement me
	panic("implement me")
}

func NewMappingEntityIterator(
	mappings []*PropertyMapping,
	itemIterator ItemIterator,
	customMapping func(mapping *PropertyMapping, item Item, entity egdm.Entity),
) *MappingEntityIterator {
	return &MappingEntityIterator{
		mapper:        NewGenericEntityMapper(mappings),
		rowIterator:   itemIterator,
		customMapping: customMapping,
	}
}

type dataItemMapper struct {
	mappings []*PropertyMapping
}

func (d dataItemMapper) EntityToItem(entity *egdm.Entity) Item {
	defaultItem := &DataItem{raw: make(map[string]any)}
	for _, mapping := range d.mappings {
		if mapping.IsIdentity {
			defaultItem.SetValue(mapping.Property, entity.ID)
		} else {
			defaultItem.SetValue(mapping.Property, entity.Properties[mapping.EntityProperty])
		}
	}
	return defaultItem
}

func NewDataItemMapper(mappings []*PropertyMapping) EntityToItemMapper {
	return &dataItemMapper{mappings: mappings}
}

type ItemToEntityMapper interface {
	ItemToEntity(item Item) *egdm.Entity
}
type EntityToItemMapper interface {
	EntityToItem(entity *egdm.Entity) Item
}

type GenericEntityMapper struct {
	Mappings []*PropertyMapping
}

func (em *GenericEntityMapper) ItemToEntity(item Item) *egdm.Entity {
	entity := egdm.NewEntity()
	for _, mapping := range em.Mappings {
		sourcePropertyValue := item.GetValue(mapping.Property)
		if sourcePropertyValue == nil {
			continue
		}

		if mapping.IsIdentity {
			entity.ID = sourcePropertyValue.(string)
		} else {
			entity.Properties[mapping.EntityProperty] = sourcePropertyValue
		}
	}
	return entity
}

func NewGenericEntityMapper(mappings []*PropertyMapping) *GenericEntityMapper {
	return &GenericEntityMapper{
		Mappings: mappings,
	}
}
*/
