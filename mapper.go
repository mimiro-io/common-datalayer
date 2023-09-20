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
	incomingMappingConfig       *IncomingMappingConfig
	outgoingMappingConfig       *OutgoingMappingConfig
	itemToEntityCustomTransform []func(item Item, entity *egdm.Entity) error
	entityToItemCustomTransform []func(entity *egdm.Entity, item Item) error
}

func NewMapper(logger Logger, incomingMappingConfig *IncomingMappingConfig, outgoingMappingConfig *OutgoingMappingConfig) *Mapper {
	mapper := &Mapper{
		logger:                      logger,
		incomingMappingConfig:       incomingMappingConfig,
		outgoingMappingConfig:       outgoingMappingConfig,
		itemToEntityCustomTransform: make([]func(item Item, entity *egdm.Entity) error, 0),
		entityToItemCustomTransform: make([]func(entity *egdm.Entity, item Item) error, 0),
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

	for _, construction := range mapper.outgoingMappingConfig.Constructions {
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
	if mapper.outgoingMappingConfig.MapAll {
		for _, propertyName := range item.GetPropertyNames() {
			mappedProperties[propertyName] = false
		}
	}

	// apply mappings
	for _, mapping := range mapper.outgoingMappingConfig.PropertyMappings {

		if mapping.Property == "" {
			return fmt.Errorf("property name is required")
		}

		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty
		if mapper.outgoingMappingConfig.MapAll {
			mappedProperties[propertyName] = true
		}

		propertyValue, err := getValueFromItemOrConstruct(item, propertyName, constructedProperties)
		if err != nil {
			return errors.Wrap(err, "failed to get value from item or construct")
		}
		if propertyValue == nil {
			if mapping.DefaultValue != nil {
				propertyValue = mapping.DefaultValue
			} else {
				continue
			}
		}

		if mapping.IsIdentity {
			idValue, err := stringOfValue(propertyValue)
			if err != nil {
				return errors.Wrap(err, "failed to convert identity value to string")
			}
			if mapping.UrlValuePattern == "" {
				return fmt.Errorf("url value pattern is required for identity property")
			}
			entity.ID = makeURL(mapping.UrlValuePattern, idValue)
		} else if mapping.IsReference {
			// reference property
			referenceValue, err := stringOfValue(propertyValue)
			if err != nil {
				return errors.Wrap(err, "failed to convert reference value to string")
			}
			entity.References[entityPropertyName] = makeURL(mapping.UrlValuePattern, referenceValue)
		} else {
			// regular property
			entity.Properties[entityPropertyName] = propertyValue
		}
	}

	// iterate over unmapped properties and add them to the entity
	for propertyName, mapped := range mappedProperties {
		if !mapped {
			propertyValue := item.GetValue(propertyName)
			entityPropertyName := mapper.outgoingMappingConfig.BaseURI + propertyName
			entity.Properties[entityPropertyName] = propertyValue
		}
	}

	// apply custom transforms
	if len(mapper.itemToEntityCustomTransform) > 0 {
		for _, transform := range mapper.itemToEntityCustomTransform {
			err := transform(item, entity)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getValueFromItemOrConstruct(item Item, propertyName string, constructedProperties map[string]any) (any, error) {
	if val, ok := constructedProperties[propertyName]; ok {
		return val, nil
	} else {
		return item.GetValue(propertyName), nil
	}
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

	mappedProperties := make(map[string]bool)
	if mapper.incomingMappingConfig.MapAll {
		for _, propertyName := range item.GetPropertyNames() {
			mappedProperties[propertyName] = false
		}
	}

	for _, mapping := range mapper.incomingMappingConfig.PropertyMappings {
		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty

		if mapping.IsIdentity {
			item.SetValue(propertyName, entity.ID)
		} else if mapping.IsReference {
			// reference property
			referenceValue := entity.References[entityPropertyName]
			item.SetValue(propertyName, referenceValue)
		} else {
			// regular property
			propertyValue := entity.Properties[entityPropertyName]
			item.SetValue(propertyName, propertyValue)
		}
	}

	// apply custom transforms
	if len(mapper.entityToItemCustomTransform) > 0 {
		for _, transform := range mapper.entityToItemCustomTransform {
			err := transform(entity, item)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type Item interface {
	// GetValue returns the value of the property with the given name
	GetValue(name string) any
	// SetValue sets the value of the property with the given name
	SetValue(name string, value any)
	// NativeItem returns the underlying native item
	NativeItem() any
	// GetPropertyNames returns the names of all properties in the item
	GetPropertyNames() []string
}
