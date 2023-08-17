package common_datalayer

import (
	egdm "github.com/mimiro-io/entity-graph-data-model"
)

type Item interface {
	GetRaw() map[string]any
	PutRaw(raw map[string]any)
	GetValue(name string) any
	SetValue(name string, value any)
}

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