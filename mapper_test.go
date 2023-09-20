package common_datalayer

import (
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"testing"
)

type InMemoryItem struct {
	properties    map[string]interface{}
	propertyNames []string
}

func (i *InMemoryItem) GetPropertyNames() []string {
	return i.propertyNames
}

func (i *InMemoryItem) GetValue(name string) any {
	return i.properties[name]
}

func (i *InMemoryItem) SetValue(name string, value interface{}) {
	i.properties[name] = value
}

func (i *InMemoryItem) NativeItem() any {
	return i.properties
}

func TestMapOutgoingItemWithIdentity(t *testing.T) {
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:        "id",
		Datatype:        "string",
		IsIdentity:      true,
		UrlValuePattern: "http://data.example.com/{value}",
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := &egdm.Entity{}
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}
}

func TestMapOutgoingItemWithIdentityButMissingUrlPattern(t *testing.T) {
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:   "id",
		Datatype:   "string",
		IsIdentity: true,
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := &egdm.Entity{}
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing url pattern")
		t.Failed()
	}

	if err.Error() != "url value pattern is required for identity property" {
		t.Error("should have failed with missing url pattern")
	}
}

func TestMapOutgoingItemWithIdentityButMissingValue(t *testing.T) {
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:        "id",
		Datatype:        "string",
		IsIdentity:      true,
		UrlValuePattern: "http://data.example.com/{value}",
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := &egdm.Entity{}
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing value")
		t.Failed()
	}

	if err.Error() != "identity property not found" {
		t.Error("should have failed with missing value")
	}
}
