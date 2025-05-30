package common_datalayer

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	egdm "github.com/mimiro-io/entity-graph-data-model"
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

func TestOutgoingMappingWithBadBaseUri(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema",
	}

	mapper := NewMapper(logger, nil, outgoingConfig)

	if mapper.outgoingMappingConfig.BaseURI != "http://data.example.com/schema/" {
		t.Error("base uri should have a trailing slash")
	}

	outgoingConfig = &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema#",
	}

	mapper = NewMapper(logger, nil, outgoingConfig)

	if mapper.outgoingMappingConfig.BaseURI != "http://data.example.com/schema#" {
		t.Error("base uri should have a trailing hash")
	}

	outgoingConfig = &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	mapper = NewMapper(logger, nil, outgoingConfig)

	if mapper.outgoingMappingConfig.BaseURI != "http://data.example.com/schema/" {
		t.Error("base uri should have a trailing slash")
	}
}

func TestIncomingMappingWithBadBaseUri(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{}

	mapper := NewMapper(logger, incomingConfig, nil)
	if mapper == nil {
		t.Error("mapper should not be nil")
	}

	incomingConfig = &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema",
	}

	mapper = NewMapper(logger, incomingConfig, nil)

	if mapper.incomingMappingConfig.BaseURI != "http://data.example.com/schema/" {
		t.Error("base uri should have a trailing slash")
	}

	incomingConfig = &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema#",
	}

	mapper = NewMapper(logger, incomingConfig, nil)

	if mapper.incomingMappingConfig.BaseURI != "http://data.example.com/schema#" {
		t.Error("base uri should have a trailing hash")
	}

	incomingConfig = &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	mapper = NewMapper(logger, incomingConfig, nil)

	if mapper.incomingMappingConfig.BaseURI != "http://data.example.com/schema/" {
		t.Error("base uri should have a trailing slash")
	}
}

func TestMapOutgoingItemWithIdentity(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:        "id",
		IsIdentity:      true,
		URIValuePattern: "http://data.example.com/{value}",
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
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:   "id",
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

	if !strings.Contains(err.Error(), "url value pattern is required for identity property") {
		t.Error("should have failed with missing url pattern")
	}
}

func TestMapOutgoingItemWithIdentityButMissingValue(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings, &ItemToEntityPropertyMapping{
		Property:        "id",
		IsIdentity:      true,
		URIValuePattern: "http://data.example.com/{value}",
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := &egdm.Entity{}
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing property")
	}

	if !strings.Contains(err.Error(), "property id is required") {
		t.Error("wrong error message")
	}
}

// test outgoing property mapping
func TestMapOutgoingItemWithPropertyMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "name",
			EntityProperty: "http://data.example.com/name",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("name", "homer")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.Properties["http://data.example.com/name"] != "homer" {
		t.Error("entity property name should be homer")
	}
}

func TestMapOutgoingItemWithPropertyMappingOfDifferentTypes(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "tags",
			EntityProperty: "http://data.example.com/tags",
		},
		&ItemToEntityPropertyMapping{
			Property:       "name",
			EntityProperty: "http://data.example.com/name",
		},
		&ItemToEntityPropertyMapping{
			Property:       "ratings",
			EntityProperty: "http://data.example.com/ratings",
		},
		&ItemToEntityPropertyMapping{
			Property:       "height",
			EntityProperty: "http://data.example.com/height",
		},
		&ItemToEntityPropertyMapping{
			Property:        "when",
			IsReference:     true,
			EntityProperty:  "http://data.example.com/when",
			URIValuePattern: "http://data.example.com/when/{value}",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("height", 1.92)
	item.SetValue("name", "homer")
	item.SetValue("tags", []string{"marge", "simpson"})
	item.SetValue("ratings", []float64{1.0, 5.0})
	item.SetValue("id", "1")
	item.SetValue("when", time.Unix(1703063199, 0))

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.Properties["http://data.example.com/name"] != "homer" {
		t.Error("entity property name should be homer")
	}

	if entity.Properties["http://data.example.com/height"] != 1.92 {
		t.Error("entity property height should be 1.92")
	}

	if entity.Properties["http://data.example.com/tags"].([]string)[0] != "marge" {
		t.Error("entity property tags should be [\"marge\", \"simpson\"]")
	}

	if entity.Properties["http://data.example.com/ratings"].([]float64)[0] != 1.0 {
		t.Error("entity property ratings should be [1.0, 5.0]")
	}

	expectedTime := time.Unix(1703063199, 0).Format(time.RFC3339)
	if entity.References["http://data.example.com/when"] != "http://data.example.com/when/"+expectedTime {
		t.Error("entity reference when should be http://data.example.com/when/"+expectedTime+", was ",
			entity.References["http://data.example.com/when"])
	}
}

func TestMapOutgoingWithChainedConstructions(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	mapper := NewMapper(logger, nil, &OutgoingMappingConfig{
		Constructions: []*PropertyConstructor{
			{
				PropertyName: "what",
				Operation:    "trim",
				Arguments:    []string{"what"},
			},
			{
				PropertyName: "what",
				Operation:    "toupper",
				Arguments:    []string{"what"},
			},
			{
				PropertyName: "dash",
				Operation:    "literal",
				Arguments:    []string{"-"},
			},
			{
				PropertyName: "what",
				Operation:    "concat",
				Arguments:    []string{"what", "dash"},
			},
			{
				PropertyName: "when",
				Operation:    "replace",
				Arguments:    []string{"when", "-", "_"},
			},
			{
				PropertyName: "when",
				Operation:    "replace",
				Arguments:    []string{"when", ":", "_"},
			},
			{
				PropertyName: "when",
				Operation:    "replace",
				Arguments:    []string{"when", "+", "_"},
			},
			{
				PropertyName: "id",
				Operation:    "concat",
				Arguments:    []string{"what", "when"},
			},
		},
		PropertyMappings: []*ItemToEntityPropertyMapping{{
			Property:        "id",
			URIValuePattern: "http://data.example.com/id/{value}",
			IsIdentity:      true,
		}},
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("what", "   Birthday ")
	item.SetValue("when", time.Unix(1703063199, 0))

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	expectedTime := time.Unix(1703063199, 0).Format(time.RFC3339)
	expectedTime = strings.ReplaceAll(expectedTime, ":", "_")
	expectedTime = strings.ReplaceAll(expectedTime, "-", "_")
	expectedTime = strings.ReplaceAll(expectedTime, "+", "_")
	if entity.ID != "http://data.example.com/id/BIRTHDAY-"+expectedTime {
		t.Error("entity ID should be http://data.example.com/id/BIRTHDAY-"+expectedTime+". was ", entity.ID)
	}
}

// test missing property defined as required
func TestMapOutgoingItemWithMissingRequiredProperty(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "name",
			EntityProperty: "http://data.example.com/name",
			Required:       true,
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing required property")
	}

	if !strings.Contains(err.Error(), "property name is required") {
		t.Error("wrong error message")
	}
}

// test missing entityproperty name for property mapping
func TestMapOutgoingItemWithMissingEntityPropertyNameForPropertyMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property: "name",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")
	item.SetValue("name", "homer")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing entity property name")
	}

	if !strings.Contains(err.Error(), "entity property name is required for mapping") {
		t.Error("wrong error message")
	}
}

// Test reference mapping
func TestMapOutgoingItemWithReferenceMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "company",
			IsReference:     true,
			EntityProperty:  "http://data.example.com/company",
			URIValuePattern: "http://data.example.com/companies/{value}",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")
	item.SetValue("company", "acmecorp")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.References["http://data.example.com/company"] != "http://data.example.com/companies/acmecorp" {
		t.Error("entity reference company should be http://data.example.com/companies/acmecorp")
	}
}

// Test reference mapping with list of values
func TestMapOutgoingItemWithReferenceMappingWithListOfValues(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "company",
			IsReference:     true,
			EntityProperty:  "http://data.example.com/company",
			URIValuePattern: "http://data.example.com/companies/{value}",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("id", "1")
	item.SetValue("company", []string{"acmecorp", "meprosoft"})

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.References["http://data.example.com/company"].([]string)[0] != "http://data.example.com/companies/acmecorp" {
		t.Error("entity reference company should be http://data.example.com/companies/acmecorp")
	}

	if entity.References["http://data.example.com/company"].([]string)[1] != "http://data.example.com/companies/meprosoft" {
		t.Error("entity reference company should be http://data.example.com/companies/acmecorp")
	}
}

// Test Map all properties
func TestMapOutgoingItemWithMapAllProperties(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		MapAll:           true,
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*ItemToEntityPropertyMapping, 0),
	}

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"name", "id"}}
	item.SetValue("name", "homer")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be 1")
	}

	if entity.Properties["http://data.example.com/schema/name"] != "homer" {
		t.Error("entity property name should be homer")
	}

	if entity.Properties["http://data.example.com/schema/id"] != "1" {
		t.Error("entity property id should be 1")
	}

	if len(entity.Properties) != 2 {
		t.Error("entity should have 2 properties")
	}
}

func TestMapOutgoingItemWithSubItemMapAll(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		MapAll:           true,
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*ItemToEntityPropertyMapping, 0),
	}

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"name", "id", "orders"}}
	item.SetValue("name", "homer")
	item.SetValue("id", "1")

	subItem := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"id", "cost"}}
	subItem.SetValue("id", "10")
	subItem.SetValue("cost", 100.0)

	item.SetValue("orders", []Item{subItem})

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be 1")
	}

	if entity.Properties["http://data.example.com/schema/name"] != "homer" {
		t.Error("entity property name should be homer")
	}

	if entity.Properties["http://data.example.com/schema/id"] != "1" {
		t.Error("entity property name should be homer")
	}

	if entity.Properties["http://data.example.com/schema/orders"].([]*egdm.Entity)[0].Properties["http://data.example.com/schema/id"] != "10" {
		t.Error("nested sub entity should get Entity type and get the same BaseURI namespace as the main properties")
	}

	if len(entity.Properties) != 3 {
		t.Error("entity should have 3 properties")
	}
}

func TestMapOutgoingItemWithSubItemPropertyMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "address",
			EntityProperty: "homeAddress",
		},
		&ItemToEntityPropertyMapping{
			Property:       "street",
			EntityProperty: "street",
		},
		&ItemToEntityPropertyMapping{
			Property:       "city",
			EntityProperty: "city",
		},
		&ItemToEntityPropertyMapping{
			Property:       "name",
			EntityProperty: "firstName",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"name", "id", "address"}}
	item.SetValue("name", "homer")
	item.SetValue("id", "1")

	subItem := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"id", "street", "city"}}
	subItem.SetValue("id", "10")
	subItem.SetValue("street", "Main street")
	subItem.SetValue("city", "Springfield")

	item.SetValue("address", subItem)

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.Properties["http://data.example.com/schema/firstName"] != "homer" {
		t.Error("entity property firstName should be homer")
	}

	homeAddress, ok := entity.Properties["http://data.example.com/schema/homeAddress"].(*egdm.Entity)
	if !ok {
		t.Error("nested sub entity should get Entity type")
	} else {
		if homeAddress.Properties["http://data.example.com/schema/street"] != "Main street" {
			t.Error("nested sub entity should get Entity type and get the same BaseURI namespace as the main properties")
		}
		if homeAddress.Properties["http://data.example.com/schema/city"] != "Springfield" {
			t.Error("nested sub entity should get Entity type and get the same BaseURI namespace as the main properties")
		}
	}

	if entity.Properties["http://data.example.com/schema/homeAddress"].(*egdm.Entity).Properties["http://data.example.com/schema/city"] != "Springfield" {
		t.Error("nested sub entity should get Entity type and get the same BaseURI namespace as the main properties")
	}

	if len(entity.Properties) != 2 {
		t.Error("entity should have 2 properties")
	}
}

func TestMapOutgoingItemWithDeletedProperty(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		PropertyMappings: []*ItemToEntityPropertyMapping{
			{
				Property:  "is_removed",
				IsDeleted: true,
			},
		},
	}

	item := &InMemoryItem{
		properties:    map[string]any{"is_removed": true},
		propertyNames: []string{"is_removed"},
	}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.IsDeleted != true {
		t.Error("entity should be deleted")
	}
}

func TestMapOutgoingItemWithWrongDeletedProperty(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		PropertyMappings: []*ItemToEntityPropertyMapping{
			{
				Property:  "name",
				IsDeleted: true,
			},
		},
	}

	item := &InMemoryItem{
		properties:    map[string]any{"name": "Hans"},
		propertyNames: []string{"name"},
	}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with wrong value type")
	}

	if !strings.Contains(err.Error(), "IsDeleted property 'name' must be a bool") {
		t.Error("wrong error message")
	}
}

func TestMapOutgoingItemWithRecordedProperty(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		PropertyMappings: []*ItemToEntityPropertyMapping{
			{
				Property:   "recorded",
				IsRecorded: true,
			},
		},
	}

	item := &InMemoryItem{
		properties:    map[string]any{"recorded": 165455645554477},
		propertyNames: []string{"recorded"},
	}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.Recorded != 165455645554477 {
		t.Error("entity should have recorded value 165455645554477")
	}
}

func TestMapOutgoingItemWithWrongRecordedProperty(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		PropertyMappings: []*ItemToEntityPropertyMapping{
			{
				Property:   "name",
				IsRecorded: true,
			},
		},
	}

	item := &InMemoryItem{
		properties:    map[string]any{"name": "Hans"},
		propertyNames: []string{"name"},
	}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with wrong value type")
	}

	if !strings.Contains(err.Error(), "IsRecorded property 'name' must be a uint64 (unix timestamp)") {
		println(err.Error())
		t.Error("wrong error message")
	}
}

// test outgoing property mapping
func TestMapOutgoingItemWithDefaultRdfType(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)
	outgoingConfig.DefaultType = "http://example.com/types/Person"

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("name", "homer")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.References["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"] != "http://example.com/types/Person" {
		t.Error("entity reference type should be http://example.com/types/Person")
	}
}

// Test Incoming property mapping
func TestMapIncomingItemWithPropertyMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*EntityToItemPropertyMapping, 0),
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty: "http://data.example.com/schema/name",
			Property:       "name",
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.Properties["http://data.example.com/schema/name"] = "homer"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("name") != "homer" {
		t.Error("item property name should be homer")
	}
}
func TestMapIncomingItemWithPropertyMappingDefaultValue(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*EntityToItemPropertyMapping, 0),
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty: "http://data.example.com/schema/name",
			Property:       "name",
			DefaultValue:   "defaultName",
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("name") != "defaultName" {
		t.Error("item property name should be defaultName")
	}
}

func TestMapIncomingItemWithBaseURIPropertyMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*EntityToItemPropertyMapping, 0),
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty: "name",
			Property:       "name",
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.Properties["http://data.example.com/schema/name"] = "homer"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("name") != "homer" {
		t.Error("item property name should be homer")
	}
}

// Test Incoming property mapping with MapNamed
func TestMapIncomingItemWithPropertyMappingAndMapNamed(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI:  "http://data.example.com/schema/",
		MapNamed: true,
	}

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.Properties["http://data.example.com/schema/name"] = "homer"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"name", "id", "dob"}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("name") != "homer" {
		t.Error("item property name should be homer")
	}
}

// Test Incoming property mapping with reference mappings
func TestMapIncomingItemWithReferenceMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty:       "http://data.example.com/schema/company",
			Property:             "company",
			IsReference:          true,
			StripReferencePrefix: true,
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.References["http://data.example.com/schema/company"] = "http://data.example.com/companies/acmecorp"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"company", "id"}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("company") != "acmecorp" {
		t.Error("item property company should be acmecorp")
	}
}
func TestMapIncomingItemWithReferenceMappingDefaultValue(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty:       "http://data.example.com/schema/company",
			Property:             "company",
			IsReference:          true,
			StripReferencePrefix: true,
			DefaultValue:         "defaultCompany",
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"company", "id"}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("company") != "defaultCompany" {
		t.Error("item property company should be defaultCompany")
	}
}

// Test Incoming property mapping with array of reference mappings
func TestMapIncomingItemWithReferenceArrayMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			EntityProperty:       "http://data.example.com/schema/company",
			Property:             "company",
			IsReference:          true,
			StripReferencePrefix: true,
		},
		&EntityToItemPropertyMapping{
			Property:             "id",
			IsIdentity:           true,
			StripReferencePrefix: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.ID = "http://data.example.com/1"
	entity.References["http://data.example.com/schema/company"] = []string{"http://data.example.com/companies/acmecorp", "http://data.example.com/companies/meprosoft"}
	entity.Properties["http://data.example.com/schema/id"] = "1"

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{"company", "id"}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("id") != "1" {
		t.Error("item property id should be 1")
	}

	if item.GetValue("company").([]string)[0] != "acmecorp" {
		t.Error("item property company should be acmecorp")
	}

	if item.GetValue("company").([]string)[1] != "meprosoft" {
		t.Error("item property company should be meprosoft")
	}
}

func TestMapIncomingItemWithDeletedMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			Property:  "is_removed",
			IsDeleted: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.IsDeleted = true

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("is_removed") != true {
		t.Error("item property is_removed should be true")
	}
}

func TestMapIncomingItemWithRecordedMapping(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	incomingConfig := &IncomingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}

	incomingConfig.PropertyMappings = append(incomingConfig.PropertyMappings,
		&EntityToItemPropertyMapping{
			Property:   "recorded_ts",
			IsRecorded: true,
		})

	// make the entity
	entity := egdm.NewEntity()
	entity.Recorded = 1645554566455

	mapper := NewMapper(logger, incomingConfig, nil)

	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: []string{}}
	err := mapper.MapEntityToItem(entity, item)
	if err != nil {
		t.Error(err)
	}

	if item.GetValue("recorded_ts") != uint64(1645554566455) {
		t.Error("item property recorded_ts should be 1645554566455")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCasting(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "age",
			EntityProperty: "age",
			Datatype:       "int",
		},
		&ItemToEntityPropertyMapping{
			Property:       "phone",
			EntityProperty: "phone",
			Datatype:       "long",
		},
		&ItemToEntityPropertyMapping{
			Property:       "height",
			EntityProperty: "height",
			Datatype:       "float",
		},
		&ItemToEntityPropertyMapping{
			Property:       "weight",
			EntityProperty: "weight",
			Datatype:       "double",
		},
		&ItemToEntityPropertyMapping{
			Property:       "male",
			EntityProperty: "male",
			Datatype:       "bool",
		},
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "long",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("age", "42")
	item.SetValue("phone", "31474836")
	item.SetValue("height", "1.92")
	item.SetValue("weight", "70.9")
	item.SetValue("male", 1)
	item.SetValue("accountNumber", "3147483647")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err != nil {
		t.Error(err)
	}

	if entity.ID != "http://data.example.com/1" {
		t.Error("entity ID should be http://data.example.com/1")
	}

	if entity.Properties["http://data.example.com/schema/age"] != 42 {
		t.Error("entity property age should be 42 as integer")
	}

	var phone int64 = 31474836
	if entity.Properties["http://data.example.com/schema/phone"] != phone {
		t.Errorf("entity property phone should be %d as integer", phone)
	}

	var height float32 = 1.92
	if entity.Properties["http://data.example.com/schema/height"] != height {
		t.Error("entity property height should be 1.92 as float32")
	}

	var weight float64 = 70.9
	if entity.Properties["http://data.example.com/schema/weight"] != weight {
		t.Error("entity property weight should be 70.9 as float64")
	}

	if entity.Properties["http://data.example.com/schema/male"] != true {
		t.Error("entity property male should be true as bool")
	}

	var accountNumber int64 = 3147483647
	if entity.Properties["http://data.example.com/schema/accountNumber"] != accountNumber {
		t.Error("entity property accountNumber should be 3147483647 as int64")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCastingOverIntLimit(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "int",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("accountNumber", "814748364788888")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("Should have failed with integer out of range")
		t.FailNow()
	}

	if !strings.Contains(err.Error(), "error: value out of range for int type") {
		println(err.Error())
		t.Error("wrong error message")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCastingOverIntLimit2(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "int",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("accountNumber", 814748364788888)
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("Should have failed with integer out of range")
		t.FailNow()
	}

	if !strings.Contains(err.Error(), "error: value out of range for int type") {
		println(err.Error())
		t.Error("wrong error message")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCastingOverFloatLimit(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "float",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("accountNumber", fmt.Sprintf("%f", math.MaxFloat32*5))
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("Should have failed with float out of range")
		t.FailNow()
	}

	if !strings.Contains(err.Error(), "error: value out of range for Float32 type") {
		println(err.Error())
		t.Error("wrong error message")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCastingOverFloatLimit2(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "float",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("accountNumber", math.MaxFloat32*5)
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("Should have failed with float out of range")
		t.FailNow()
	}

	if !strings.Contains(err.Error(), "error: value out of range for Float32 type") {
		println(err.Error())
		t.Error("wrong error message")
	}
}

func TestMapOutgoingItemWithPropertyDatatypeCastingInvalidValue(t *testing.T) {
	logger := NewLogger("testService", "text", "info")

	outgoingConfig := &OutgoingMappingConfig{
		BaseURI: "http://data.example.com/schema/",
	}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:       "accountNumber",
			EntityProperty: "accountNumber",
			Datatype:       "float",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			URIValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("accountNumber", "banana")
	item.SetValue("id", "1")

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := egdm.NewEntity()
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("Should have failed with invalid syntax")
		t.FailNow()
	}

	if !strings.Contains(err.Error(), "error: strconv.ParseFloat: parsing \"banana\": invalid syntax") {
		println(err.Error())
		t.Error("wrong error message")
	}
}
