package common_datalayer

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	egdm "github.com/mimiro-io/entity-graph-data-model"
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

	// ensure base URI ends with /
	mapper.verifyBaseUri()

	return mapper
}

func (mapper *Mapper) verifyBaseUri() {
	if mapper.incomingMappingConfig != nil && mapper.incomingMappingConfig.BaseURI != "" {
		if !strings.HasSuffix(mapper.incomingMappingConfig.BaseURI, "/") && !strings.HasSuffix(mapper.incomingMappingConfig.BaseURI, "#") {
			mapper.incomingMappingConfig.BaseURI = mapper.incomingMappingConfig.BaseURI + "/"
		}
	}
	if mapper.outgoingMappingConfig != nil && mapper.outgoingMappingConfig.BaseURI != "" {
		if !strings.HasSuffix(mapper.outgoingMappingConfig.BaseURI, "/") && !strings.HasSuffix(mapper.outgoingMappingConfig.BaseURI, "#") {
			mapper.outgoingMappingConfig.BaseURI = mapper.outgoingMappingConfig.BaseURI + "/"
		}
	}
}

func (mapper *Mapper) WithEntityToItemTransform(transform func(entity *egdm.Entity, item Item) error) *Mapper {
	mapper.entityToItemCustomTransform = append(mapper.entityToItemCustomTransform, transform)
	return mapper
}

func (mapper *Mapper) WithItemToEntityTransform(transform func(item Item, entity *egdm.Entity) error) *Mapper {
	mapper.itemToEntityCustomTransform = append(mapper.itemToEntityCustomTransform, transform)
	return mapper
}

type mutableItem struct {
	item                  Item
	constructedProperties map[string]any
}

func (m *mutableItem) GetValue(name string) any {
	val, err := getValueFromItemOrConstruct(m.item, name, m.constructedProperties)
	if err != nil {
		return nil
	}
	return val
}

func (m *mutableItem) SetValue(name string, value any) { m.item.SetValue(name, value) }

func (m *mutableItem) NativeItem() any { return m.item.NativeItem() }

func (m *mutableItem) GetPropertyNames() []string { return m.item.GetPropertyNames() }

func (mapper *Mapper) MapItemToEntity(item Item, entity *egdm.Entity) error {
	// apply constructions
	constructedProperties := make(map[string]any)
	item = &mutableItem{item, constructedProperties}
	if mapper.outgoingMappingConfig == nil {
		return fmt.Errorf("outgoing mapping config is nil")
	}

	if mapper.outgoingMappingConfig.Constructions != nil {
		for _, construction := range mapper.outgoingMappingConfig.Constructions {
			switch construction.Operation {
			case "concat":
				if len(construction.Arguments) != 2 {
					return fmt.Errorf("concat operation requires two arguments")
				}
				concatedValue, err := concat(item, construction.Arguments[0], construction.Arguments[1])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = concatedValue
			case "split":
				if len(construction.Arguments) != 2 {
					return fmt.Errorf("split operation requires two arguments")
				}
				splitValue, err := split(item, construction.Arguments[0], construction.Arguments[1])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = splitValue
			case "replace":
				if len(construction.Arguments) != 3 {
					return fmt.Errorf("replace operation requires three arguments")
				}
				replacedValue, err := replace(item, construction.Arguments[0], construction.Arguments[1], construction.Arguments[2])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = replacedValue
			case "trim":
				if len(construction.Arguments) != 1 {
					return fmt.Errorf("trim operation requires one argument")
				}
				trimmedValue, err := trim(item, construction.Arguments[0])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = trimmedValue
			case "tolower":
				if len(construction.Arguments) != 1 {
					return fmt.Errorf("tolower operation requires one argument")
				}
				tolowerValue, err := tolower(item, construction.Arguments[0])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = tolowerValue
			case "toupper":
				if len(construction.Arguments) != 1 {
					return fmt.Errorf("toupper operation requires one argument")
				}
				toupperValue, err := toupper(item, construction.Arguments[0])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = toupperValue
			case "regex":
				if len(construction.Arguments) != 2 {
					return fmt.Errorf("regex operation requires two arguments")
				}
				regexValue, err := regex(item, construction.Arguments[0], construction.Arguments[1])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = regexValue
			case "slice":
				if len(construction.Arguments) != 3 {
					return fmt.Errorf("slice operation requires three arguments")
				}
				slicedValue, err := slice(item, construction.Arguments[0], construction.Arguments[1], construction.Arguments[2])
				if err != nil {
					return err
				}
				constructedProperties[construction.PropertyName] = slicedValue
			case "literal":
				if len(construction.Arguments) != 1 {
					return fmt.Errorf("literal operation requires one argument")
				}
				constructedProperties[construction.PropertyName] = construction.Arguments[0]
			}
		}
	}

	if mapper.outgoingMappingConfig.MapAll {
		// iterate over unmapped properties and add them to the entity
		for _, propertyName := range item.GetPropertyNames() {
			propertyValue := item.GetValue(propertyName)
			entityPropertyName := mapper.outgoingMappingConfig.BaseURI + propertyName
			entity.Properties[entityPropertyName] = propertyValue
		}
	}

	// apply mappings
	for _, mapping := range mapper.outgoingMappingConfig.PropertyMappings {

		if mapping.Property == "" {
			return fmt.Errorf("property name is required, mapping: %+v", mapping)
		}

		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty
		if !strings.HasPrefix(entityPropertyName, "http") && entityPropertyName != "" {
			if mapper.outgoingMappingConfig.BaseURI == "" {
				return fmt.Errorf("base uri is required for mapping and entity_property isnt full URI. mapping: %+v", mapping)
			}
			entityPropertyName = mapper.outgoingMappingConfig.BaseURI + entityPropertyName
		}

		propertyValue, err := getValueFromItemOrConstruct(item, propertyName, constructedProperties)
		if err != nil {
			return fmt.Errorf("failed to get value from item or construct. item: %+v, error: %w", item.NativeItem(), err)
		}
		if propertyValue == nil {
			if mapping.DefaultValue != nil {
				propertyValue = mapping.DefaultValue
			} else {
				if mapping.Required || mapping.IsIdentity {
					return fmt.Errorf("property %s is required. item: %+v", propertyName, item.NativeItem())
				}
				continue
			}
		}

		if mapping.IsIdentity {
			idValue, err := stringOfValue(propertyValue)
			if err != nil {
				return fmt.Errorf("failed to convert identity value to string. item: %+v, error: %w", item.NativeItem(), err)
			}
			if mapping.URIValuePattern == "" {
				return fmt.Errorf("url value pattern is required for identity property. mapping: %+v", mapping)
			}
			entity.ID = makeURL(mapping.URIValuePattern, idValue)
		} else if mapping.IsReference {
			if entityPropertyName == "" {
				return fmt.Errorf("entity property name is required for mapping. mapping: %+v", mapping)
			}

			// reference property
			var entityPropertyValue any

			switch v := propertyValue.(type) {
			case []string:
				entityPropertyValue = make([]string, len(v))
				for i, val := range v {
					s, err := stringOfValue(val)
					if err != nil {
						return fmt.Errorf("failed to convert reference value to string value: %+v, item: %+v,error: %w", val, item.NativeItem(), err)
					}
					entityPropertyValue.([]string)[i] = makeURL(mapping.URIValuePattern, s)
				}
			default:
				s, err := stringOfValue(propertyValue)
				if err != nil {
					return fmt.Errorf("failed to convert reference value to string value: %+v, item: %+v,error: %w", propertyValue, item.NativeItem(), err)
				}
				entityPropertyValue = makeURL(mapping.URIValuePattern, s)
			}

			entity.References[entityPropertyName] = entityPropertyValue
		} else if mapping.IsDeleted {
			if boolVal, ok := propertyValue.(bool); ok {
				entity.IsDeleted = boolVal
			} else {
				return fmt.Errorf("IsDeleted property '%v' must be a bool. item: %+v", propertyName, item.NativeItem())
			}
		} else if mapping.IsRecorded {
			intVal, err := intOfValue(propertyValue)
			if err != nil {
				return fmt.Errorf("IsRecorded property '%v' must be a uint64 (unix timestamp), item: %+v, error: %w", propertyName, item.NativeItem(), err)
			}
			entity.Recorded = uint64(intVal)
		} else {
			if entityPropertyName == "" {
				return fmt.Errorf("entity property name is required for mapping: %+v", mapping)
			}

			// regular property
			entity.Properties[entityPropertyName] = propertyValue
		}
	}

	// apply custom transforms
	if len(mapper.itemToEntityCustomTransform) > 0 {
		for _, transform := range mapper.itemToEntityCustomTransform {
			err := transform(item, entity)
			if err != nil {
				return fmt.Errorf("custom transform failed. mapper: %+v, item: %+v, error: %w", mapper, item.NativeItem(), err)
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

func regex(item Item, p1 string, pattern string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("regex: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}
	// Compile the regular expression pattern
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", fmt.Errorf("regex: invalid pattern: '%s'. item: %+v, error: %w", pattern, item.NativeItem(), err)
	}

	// Find the first match in the string
	match := re.FindString(s1)
	if match == "" {
		return "", fmt.Errorf("regex: no match. pattern: '%s', property: '%s' item: %+v, error: %w", pattern, p1, item.NativeItem(), err)
	}

	return match, nil
}

func slice(item Item, p1, p2, p3 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("slice: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}

	start, err := intOfValue(p2)
	if err != nil {
		return "", fmt.Errorf("slice: start index '%s' could not be parsed. item: %+v, error: %w", p2, item.NativeItem(), err)
	}
	end, err := intOfValue(p3)
	if err != nil {
		return "", fmt.Errorf("slice: end index '%s' could not be parsed. item: %+v, error: %w", p3, item.NativeItem(), err)
	}
	return s1[start:end], nil
}

func tolower(item Item, p1 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("tolower: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}
	return strings.ToLower(s1), nil
}

func toupper(item Item, p1 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("toupper: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}
	return strings.ToUpper(s1), nil
}

func trim(item Item, p1 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("trim: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}
	return strings.TrimSpace(s1), nil
}

func replace(item Item, p1 string, s2, s3 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(p1))
	if err != nil {
		return "", fmt.Errorf("replace: property '%s' could not be accessed. item: %+v, error: %w", p1, item.NativeItem(), err)
	}
	return strings.ReplaceAll(s1, s2, s3), nil
}

func split(item Item, valueProp string, splitProp string) ([]string, error) {
	s1, err := stringOfValue(item.GetValue(valueProp))
	if err != nil {
		return nil, fmt.Errorf("split: property '%s' could not be accessed. item: %+v, error: %w", splitProp, item.NativeItem(), err)
	}

	s2, err := stringOfValue(item.GetValue(splitProp))
	if err != nil {
		return nil, fmt.Errorf("split: property '%s' could not be accessed. item: %+v, error: %w", splitProp, item.NativeItem(), err)
	}
	return strings.Split(s1, s2), nil
}

func concat(item Item, propName1, propName2 string) (string, error) {
	s1, err := stringOfValue(item.GetValue(propName1))
	if err != nil {
		return "", fmt.Errorf("concat: property '%s' could not be accessed. item: %+v, error: %w", propName1, item.NativeItem(), err)
	}
	s2, err := stringOfValue(item.GetValue(propName2))
	if err != nil {
		return "", fmt.Errorf("concat: property '%s' could not be accessed. item: %+v, error: %w", propName2, item.NativeItem(), err)
	}
	return s1 + s2, nil
}

func intOfValue(val any) (int, error) {
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
	k := t.Kind()
	switch k {
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
	case reflect.Struct:
		if t.String() == "time.Time" {
			return val.(time.Time).Format(time.RFC3339), nil
		}
		return "", fmt.Errorf("unsupported type %s (%s)", t.String(), t.Kind())
	default:
		return "", fmt.Errorf("unsupported type %s", t.Kind())
	}
}

// Takes a URL and strips away everything up to the last / or #
func stripURL(url string) string {
	if url == "" {
		return ""
	}
	lastSlash := strings.LastIndex(url, "/")
	lastHash := strings.LastIndex(url, "#")
	if lastSlash > lastHash {
		return url[lastSlash+1:]
	} else if lastHash > lastSlash {
		return url[lastHash+1:]
	} else {
		return url
	}
}

func (mapper *Mapper) MapEntityToItem(entity *egdm.Entity, item Item) error {
	// do map named as this is the more general case, then do the property mappings
	if mapper.incomingMappingConfig.MapNamed {
		for _, propertyName := range item.GetPropertyNames() {
			entityPropertyName := mapper.incomingMappingConfig.BaseURI + propertyName
			if propertyValue, ok := entity.Properties[entityPropertyName]; ok {
				item.SetValue(propertyName, propertyValue)
			}
		}
	}

	for _, mapping := range mapper.incomingMappingConfig.PropertyMappings {
		propertyName := mapping.Property
		entityPropertyName := mapping.EntityProperty
		if !strings.HasPrefix(entityPropertyName, "http") && entityPropertyName != "" {
			if mapper.incomingMappingConfig.BaseURI == "" {
				return fmt.Errorf("base uri is required for mapping and entity_property isnt full URI. mapping: %+v", mapping)
			}
			entityPropertyName = mapper.incomingMappingConfig.BaseURI + entityPropertyName
		}

		if mapping.IsIdentity {
			if mapping.StripReferencePrefix {
				item.SetValue(propertyName, stripURL(entity.ID))
			} else {
				item.SetValue(propertyName, entity.ID)
			}
		} else if mapping.IsReference {
			// reference property
			if referenceValue, ok := entity.References[entityPropertyName]; ok {
				switch v := referenceValue.(type) {
				case []string:
					values := make([]string, len(v))
					for i, val := range v {
						if mapping.StripReferencePrefix {
							values[i] = stripURL(val)
						} else {
							values[i] = val
						}
					}
					item.SetValue(propertyName, values)
				case string:
					if mapping.StripReferencePrefix {
						item.SetValue(propertyName, stripURL(v))
					} else {
						item.SetValue(propertyName, v)
					}
				default:
					return fmt.Errorf("unsupported reference type %s, value %v, entityId: %s", reflect.TypeOf(referenceValue), referenceValue, entity.ID)
				}
			}
		} else if mapping.IsDeleted {
			item.SetValue(propertyName, entity.IsDeleted)
		} else if mapping.IsRecorded {
			item.SetValue(propertyName, entity.Recorded)
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
				return fmt.Errorf("custom transform failed. mapper: %+v, entity: %+v, error: %w", mapper, entity, err)
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
