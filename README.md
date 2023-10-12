# Universal Data API - Common Data Layer Library

## Introduction

The Universal Data API is a generic RESTful API that provides a unified interface for accessing data from different data sources. It is designed to be used in a microservice architecture, where each microservice can use the Universal Data API to access data from different data sources. The Universal Data API is a RESTful API that can be used by any programming language that supports HTTP requests.

This library can be used to efficiently and consistently implement data layers over different underlying systems.

The library consists of two parts: a generic data mapper for converting back and forth between the Entity Graph Data Model and the underlying data structures. E.g. from CSV rows to and from entities.

The second part is a service hosting infrastructure that takes care of exposing the UDA API over HTTP and provides a pluggable and simple way to implement the API endpoints.

## Quick Start

The following example shows how to boostrap and start a new UDA service that exposes a data source over HTTP.

Things to note:
 - A service using the NewServiceRunner must provide a function that returns a new instance of a struct that implements the DataLayerService interface. 

``` go

package main

import cdl "github.com/mimiro-io/common-datalayer"

func main() {
	serviceRunner := cdl.NewServiceRunner(NewSampleDataLayer)
	serviceRunner.WithConfigLocation("./config")
	serviceRunner.WithEnrichConfig(EnrichConfig)
	err := serviceRunner.Start()
	if err != nil {
		panic(err)
	}
}
  
```

The DataLayerService interface is defined as follows:

``` go  

type DataLayerService interface {
    Stoppable
    UpdateConfiguration(config *Config) LayerError
    Dataset(dataset string) (Dataset, LayerError)
    DatasetNames() []string
}

```

There are obviously additional interfaces that a complete implementation must support. These include, Dataset, EntityIterator, DatasetWriter and Item. These interfaces are defined in the common_datalayer package.

For a full example see `sample/sample_data_layer.go`

## Data Layer Configuration 

A data layer instance can be configured via a number of .json files and environment variables. The service is starter with a config path location. This is the path to a folder containing the configuration files. All .json files in that folder will be loaded. 

The JSON files are merged together to define the complete config, and last update wins for any items defined with the same key.

The top level config keys are:

``` json

{
    "layer_config": {
    },
    "system_config": {
    },
    "dataset_definitions": {
    }
}

```

`layer_config` is used to configure the data layer itself. This includes the name of the data layer, the port that the layer service should expose etc. The following keys are supported:

| Field                  | Description                                         |
|------------------------|-----------------------------------------------------|
| service_name           | The name of the service                             |
| port                   | The port that the service should listen on         |
| config_refresh_interval| The interval at which the service checks for config updates |
| log_level              | The log level (one of debug, info, warn, error)    |
| log_format             | The log format (one of json, text)                 |
| statsd_enabled         | True or false, indicates if statsd should be enabled |
| statsd_agent_address   | The address of the statsd agent                    |
| custom                 | A map of custom config keys and values             |

Specific data layers are encouraged to indicate any keys and expected values that appear in the custom map in documentation.

`service_config` is used to configure information about the underlying system. This is intended to contain things like connection string, server, ports, etc. Given that this is system specific the specific keys are not specified here. It is best practice for a data layer to indicate the set of allowed keys and expected values in the documentation. 

`dataset_definitions` is used to define the datasets that the data layer exposes. The following keys are supported:

You can create a Markdown table from the data structure definition like this:

| JSON Field             | Description                                  |
|------------------------|----------------------------------------------|
| name                   | The name of the dataset                      |
| source_config          | Configuration for the data source            |
| incoming_mapping_config| Configuration for incoming data mapping      |
| outgoing_mapping_config| Configuration for outgoing data mapping      |

The `source_config` is a JSON Object and used to provide information about the dataset. This is intended to contain things like the name of the database table, any queries templates that are needed etc. 

The `incoming_mapping_config` is a JSON Object and used to provide information about how to map incoming data from the Entity to the underlying item type. The incoming mapping config is defined as follows:

| JSON Field             | Description                                         |
|------------------------|-----------------------------------------------------|
| map_named              | If true, then try and lookup entity properties based on the item property name and the BaseURI prefix |
| property_mappings      | An array of EntityToItemPropertyMapping objects     |
| base_uri               | The BaseURI prefix                                  |
| custom                 | A map of custom config keys and values              |

The EntityToItemPropertyMapping is defined as follows:

| JSON Field             | Description                                                            |
|------------------------|------------------------------------------------------------------------|
| custom                 | A map of custom config keys and values                                 |
| required               | Indicates whether the field is required                                |
| entity_property        | The entity property to map, a URL                                      |
| property               | The property to map                                                    |
| datatype               | The data type of the property                                          |
| is_reference           | Indicates whether the property is a reference                          |
| is_identity            | Indicates whether the property is an identity                          |
| default_value          | The default value for the property if property not found on the entity |
| strip_ref_prefix       | Indicates whether to strip reference value prefixes                    |

The `outgoing_mapping_config` is a JSON Object and used to provide information about how to map outgoing data from the underlying item type to the Entity. The outgoing mapping config is defined as follows:

| JSON Field             | Description                                     |
|------------------------|-------------------------------------------------|
| base_uri               | Used when mapping all properties                |
| constructions          | An array of PropertyConstructor objects         |
| property_mappings      | An array of ItemToEntityPropertyMapping objects |
| map_all                | If true, all properties are mapped              |
| custom                 | A map of custom config keys and values          |

Outgoing mappings define optional constructors and mappings. Constructors are functions that can create new properties before any mapping is applied. This can be used, for example, to concatenate multiple properties into a single property. The constructors are defined as follows:

| JSON Field | Description                       |
|------------|-----------------------------------|
| property   | The property name to construct    |
| operation  | The operation to perform          |
| args       | An array of arguments for the operation |

The following operations are supported:

| Function Name | Arguments        | Description                                        |
|---------------|------------------|----------------------------------------------------|
| regex         | arg1, arg2       | Matches a regular expression pattern arg2 in arg1  |
| slice         | arg1, arg2, arg3 | Extracts a substring from arg1 starting at arg2 and ending at arg3 |
| tolower       | arg1             | Converts arg1 to lowercase                         |
| toupper       | arg1             | Converts arg1 to uppercase                         |
| trim          | arg1             | Removes leading and trailing white spaces from arg1 |
| replace       | arg1, arg2, arg3 | Replaces all occurrences of arg2 with arg3 in arg1 |
| split         | arg1, arg2       | Splits arg1 into an array using arg2 as the delimiter |
| concat        | arg1, arg2       | Concatenates arg1 and arg2                         |

Here is a sample constructor definition:

``` json
{
    "property": "full_name",
    "operation": "concat",
    "args": [
        "firstName",
        "lastName"
    ]
}
```
After any constructors the mappings are applied, they are defined as follows:

| Field Name      | Description                                         |
|-----------------|-----------------------------------------------------|
| Custom          | A map of custom configuration keys and values       |
| Required        | Indicates whether the field is required             |
| EntityProperty  | The entity property to which the item property maps |
| Property        | The item property being mapped                      |
| Datatype        | The data type of the mapped property, optional      |
| IsReference     | Indicates whether the property is a reference       |
| URIValuePattern | The URL value pattern                               |
| IsIdentity      | Indicates whether the property is an identity       |
| DefaultValue    | The default value for the property                  |


## The Mapper

The mapper is used to convert between the Entity Graph Data Model and the underlying data structures. The concept is that the mapper can be used in any data layer implementation even of the service hosting is not used. This helps to standardise the way mappings are defined across many different kinds of data layers.

An example of the mapper in use is shown below:

``` go

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

```

