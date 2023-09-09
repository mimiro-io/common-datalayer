package common_datalayer

import (
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type Mapper struct {
	logger                      Logger
	constructions               []*PropertyConstructor
	mappings                    []*EntityPropertyMapping
	entityKeyMappings           map[string]*EntityPropertyMapping
	itemKeyMappings             map[string]*EntityPropertyMapping
	itemToEntityCustomTransform []func(item Item, entity *egdm.Entity) error
	entityToItemCustomTransform []func(entity *egdm.Entity, item Item) error
}

func NewMapper(constructions []*PropertyConstructor, mappings []*EntityPropertyMapping) *Mapper {
	mapper := &Mapper{
		mappings:                    mappings,
		constructions:               constructions,
		itemToEntityCustomTransform: make([]func(item Item, entity *egdm.Entity) error, 0),
		entityToItemCustomTransform: make([]func(entity *egdm.Entity, item Item) error, 0),
	}

	mapper.entityKeyMappings = make(map[string]*EntityPropertyMapping)
	mapper.itemKeyMappings = make(map[string]*EntityPropertyMapping)

	// enable fast lookup of the mappings
	for _, mapping := range mappings {
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

	return nil
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
	customMapping func(mapping *EntityPropertyMapping, item Item, entity egdm.Entity)
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
	mappings []*EntityPropertyMapping,
	itemIterator ItemIterator,
	customMapping func(mapping *EntityPropertyMapping, item Item, entity egdm.Entity),
) *MappingEntityIterator {
	return &MappingEntityIterator{
		mapper:        NewGenericEntityMapper(mappings),
		rowIterator:   itemIterator,
		customMapping: customMapping,
	}
}

type dataItemMapper struct {
	mappings []*EntityPropertyMapping
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

func NewDataItemMapper(mappings []*EntityPropertyMapping) EntityToItemMapper {
	return &dataItemMapper{mappings: mappings}
}

type ItemToEntityMapper interface {
	ItemToEntity(item Item) *egdm.Entity
}
type EntityToItemMapper interface {
	EntityToItem(entity *egdm.Entity) Item
}

type GenericEntityMapper struct {
	Mappings []*EntityPropertyMapping
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

func NewGenericEntityMapper(mappings []*EntityPropertyMapping) *GenericEntityMapper {
	return &GenericEntityMapper{
		Mappings: mappings,
	}
}
*/
