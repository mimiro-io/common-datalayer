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
		IsIdentity:      true,
		UrlValuePattern: "http://data.example.com/{value}",
	})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}

	mapper := NewMapper(logger, nil, outgoingConfig)

	entity := &egdm.Entity{}
	err := mapper.MapItemToEntity(item, entity)
	if err == nil {
		t.Error("should have failed with missing url pattern")
	}

	if err.Error() != "property id is required" {
		t.Error("wrong error message")
	}
}

// test outgoing property mapping
func TestMapOutgoingItemWithPropertyMapping(t *testing.T) {
	logger := newLogger("testService", "text")

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
			UrlValuePattern: "http://data.example.com/{value}",
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
	logger := newLogger("testService", "text")

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
			Property:        "id",
			IsIdentity:      true,
			UrlValuePattern: "http://data.example.com/{value}",
		})

	// make the item
	item := &InMemoryItem{properties: make(map[string]interface{}), propertyNames: make([]string, 0)}
	item.SetValue("height", 1.92)
	item.SetValue("name", "homer")
	item.SetValue("tags", []string{"marge", "simpson"})
	item.SetValue("ratings", []float64{1.0, 5.0})
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

	if entity.Properties["http://data.example.com/height"] != 1.92 {
		t.Error("entity property height should be 1.92")
	}

	if entity.Properties["http://data.example.com/tags"].([]string)[0] != "marge" {
		t.Error("entity property tags should be [\"marge\", \"simpson\"]")
	}

	if entity.Properties["http://data.example.com/ratings"].([]float64)[0] != 1.0 {
		t.Error("entity property ratings should be [1.0, 5.0]")
	}
}

// test missing property defined as required
func TestMapOutgoingItemWithMissingRequiredProperty(t *testing.T) {
	logger := newLogger("testService", "text")

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
			UrlValuePattern: "http://data.example.com/{value}",
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

	if err.Error() != "property name is required" {
		t.Error("wrong error message")
	}
}

// test missing entityproperty name for property mapping
func TestMapOutgoingItemWithMissingEntityPropertyNameForPropertyMapping(t *testing.T) {
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property: "name",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			UrlValuePattern: "http://data.example.com/{value}",
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

	if err.Error() != "entity property name is required for mapping" {
		t.Error("wrong error message")
	}
}

// Test reference mapping
func TestMapOutgoingItemWithReferenceMapping(t *testing.T) {
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "company",
			IsReference:     true,
			EntityProperty:  "http://data.example.com/company",
			UrlValuePattern: "http://data.example.com/companies/{value}",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			UrlValuePattern: "http://data.example.com/{value}",
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
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{}
	outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "company",
			IsReference:     true,
			EntityProperty:  "http://data.example.com/company",
			UrlValuePattern: "http://data.example.com/companies/{value}",
		},
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			UrlValuePattern: "http://data.example.com/{value}",
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
	logger := newLogger("testService", "text")

	outgoingConfig := &OutgoingMappingConfig{
		MapAll:           true,
		BaseURI:          "http://data.example.com/schema/",
		PropertyMappings: make([]*ItemToEntityPropertyMapping, 0),
	}

	outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
		&ItemToEntityPropertyMapping{
			Property:        "id",
			IsIdentity:      true,
			UrlValuePattern: "http://data.example.com/{value}",
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
		t.Error("entity property name should be homer")
	}

	if len(entity.Properties) != 2 {
		t.Error("entity should have 2 properties")
	}
}

// Test Incoming property mapping
func TestMapIncomingItemWithPropertyMapping(t *testing.T) {
	logger := newLogger("testService", "text")

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

func TestMapIncomingItemWithBaseURIPropertyMapping(t *testing.T) {
	logger := newLogger("testService", "text")

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
	logger := newLogger("testService", "text")

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
	logger := newLogger("testService", "text")

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

// Test Incoming property mapping with array of reference mappings
func TestMapIncomingItemWithReferenceArrayMapping(t *testing.T) {
	logger := newLogger("testService", "text")

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
