package common_datalayer

type Mapper interface {
	MapToEntity(item Item) *Entity
}

type EntityMapper struct {
	Mappings []*EntityPropertyMapping
}

func NewEntityMapper(mappings []*EntityPropertyMapping) *EntityMapper {
	return &EntityMapper{
		Mappings: mappings,
	}
}

func (em *EntityMapper) MapToEntity(item Item) *Entity {
	entity := &Entity{}
	for _, mapping := range em.Mappings {
		sourcePropertyValue := item.GetValue(mapping.Property)

		if mapping.IsIdentity {
			entity.Id = item.GetValue(mapping.Property).(string)
		} else {
			entity.Properties[mapping.EntityProperty] = item.GetValue(mapping.Property)
		}
	}
	return nil
}

func MakeUrlValue(urlValuePattern string, value interface{}) string {
	return ""
}
