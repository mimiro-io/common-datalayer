package layer

import (
	egdm "github.com/mimiro-io/entity-graph-data-model"

	"github.com/mimiro-io/common-datalayer/core"
)

type ItemToEntityMapper interface {
	ItemToEntity(item Item) *egdm.Entity
}
type EntityToItemMapper interface {
	EntityToItem(entity *egdm.Entity) Item
}

type GenericEntityMapper struct {
	Mappings []*core.EntityPropertyMapping
}

func (em *GenericEntityMapper) ItemToEntity(item Item) *egdm.Entity {
	entity := egdm.NewEntity()
	for _, mapping := range em.Mappings {
		sourcePropertyValue := item.GetValue(mapping.Property)

		if mapping.IsIdentity {
			entity.ID = sourcePropertyValue.(string)
		} else {
			entity.Properties[mapping.EntityProperty] = sourcePropertyValue
		}
	}
	return entity
}

func NewGenericEntityMapper(mappings []*core.EntityPropertyMapping) *GenericEntityMapper {
	return &GenericEntityMapper{
		Mappings: mappings,
	}
}
