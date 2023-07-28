package common_datalayer

type Entity struct {
	Id         string
	Properties map[string]interface{}
	References map[string][]string
	IsDeleted  bool
}

type Item interface {
	GetRaw() map[string]interface{}
	PutRaw(raw map[string]interface{})
	GetValue(name string) interface{}
	SetValue(name string, value interface{})
}

type ItemIterator interface {
	Next() Item
}

type EntityIterator struct {
	Mapper        Mapper
	RowIterator   ItemIterator
	CustomMapping func(mapping *EntityPropertyMapping, item Item, entity Entity)
}

func NewEntityIterator(mappings []*EntityPropertyMapping, itemIterator ItemIterator, customMapping func(mapping *EntityPropertyMapping, item Item, entity Entity)) *EntityIterator {
	return &EntityIterator{
		Mapper:        NewEntityMapper(mappings),
		RowIterator:   itemIterator,
		CustomMapping: customMapping,
	}
}

func (ei *EntityIterator) Next() *Entity {
	rawItem := ei.RowIterator.Next()
	return ei.Mapper.MapToEntity(rawItem)
}

func (ei *EntityIterator) Token() {
}

func (ei *EntityIterator) Close() {
}
