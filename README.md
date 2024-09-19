# Universal Data API - Common Data Layer Library

<!--toc:start-->

- [Universal Data API - Common Data Layer Library](#universal-data-api-common-data-layer-library)
  - [Introduction](#introduction)
  - [Quick Start](#quick-start)
  - [Data Layer Configuration](#data-layer-configuration)
    - [layer_config](#layerconfig)
    - [system_config](#systemconfig)
    - [dataset_definitions](#datasetdefinitions)
      - [source_config](#sourceconfig)
      - [incoming_mapping_config](#incomingmappingconfig)
      - [outgoing_mapping_config](#outgoingmappingconfig)
  - [The Mapper](#the-mapper)
  - [The Encoder](#the-encoder)
  <!--toc:end-->

## Introduction

The Universal Data API is a generic RESTful API that provides a unified interface for accessing data from different data sources. It is designed to be used in a microservice architecture, where each microservice can use the Universal Data API to access data from different data sources. The Universal Data API is a RESTful API that can be used by any programming language that supports HTTP requests.

This library can be used to efficiently and consistently implement data layers over different underlying systems.

The library consists of two parts: a generic data mapper for converting back and forth between the Entity Graph Data Model and the underlying data structures. E.g. from CSV rows to and from entities.

The second part is a service hosting infrastructure that takes care of exposing the UDA API over HTTP and provides a pluggable and simple way to implement the API endpoints.

## Quick Start

The following example shows how to bootstrap and start a new UDA service that exposes a data source over HTTP.

Things to note:

- A service using the NewServiceRunner must provide a function that returns a new instance of a struct that implements the DataLayerService interface.

```go
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

```go
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

```json
{
  "layer_config": {},
  "system_config": {},
  "dataset_definitions": []
}
```

### layer_config

`layer_config` is used to configure the data layer itself. This includes the name of the data layer, the port that the layer service should expose etc. The following keys are supported:

| Field                   | Description                                                 |
| ----------------------- | ----------------------------------------------------------- |
| service_name            | The name of the service                                     |
| port                    | The port that the service should listen on                  |
| config_refresh_interval | The interval at which the service checks for config updates |
| log_level               | The log level (one of debug, info, warn, error)             |
| log_format              | The log format (one of json, text)                          |
| statsd_enabled          | True or false, indicates if statsd should be enabled        |
| statsd_agent_address    | The address of the statsd agent                             |
| custom                  | A map of custom config keys and values                      |

Specific data layers are encouraged to indicate any keys and expected values that appear in the custom map in documentation.

### system_config

`system_config` is used to configure information about the underlying system. This is intended to contain things like connection string, server, ports, etc. Given that this is system specific the specific keys are not specified here. It is best practice for a data layer to indicate the set of allowed keys and expected values in the documentation.

`common-datalayer` does provide a convenience function for declaring `system_config` parameters.
`BuildNativeSystemEnvOverrides` can be plugged into the service setup via `WithEnrichConfig`.
The layer will then look for the given env vars and use them to override the json config.
Declared parameters can also be marked as required to add an additional layer of validation.

Example:

```go
cdl.NewServiceRunner(NewSampleDataLayer).
    WithConfigLocation("./config").
    WithEnrichConfig(BuildNativeSystemEnvOverrides(
        cdl.Env("db_name", true),           // required env var. will fail if neiter "db_name" in json nor "DB_NAME" in env is found
        cdl.Env("db_user", true, "dbUser"), // override jsonkey with "dbUser" but look for "DB_USER" in env
        cdl.Env("db_pwd", true),
        cdl.Env("db_timeout"), // optional env var. will not fail if missing in both json and ENV
    ))
```

### dataset_definitions

`dataset_definitions` is used to define the datasets that the data layer exposes. The following keys are supported:

You can create a Markdown table from the data structure definition like this:

| JSON Field              | Description                             |
| ----------------------- | --------------------------------------- |
| name                    | The name of the dataset                 |
| source_config           | Configuration for the data source       |
| incoming_mapping_config | Configuration for incoming data mapping |
| outgoing_mapping_config | Configuration for outgoing data mapping |

#### source_config

The `source_config` is a JSON Object and used to provide information about the dataset. This is intended to contain things like the name of the database table, any queries templates that are needed etc. Example-usage below.

##### example source_config for csv-encoded data

| JSON Field | Description                                                     |
|-----------| --------------------------------------------------------------- |
| encoding  | Specifies what type of encoding the incoming data has           |
| columns   | Names and orders the different columns of the data              |
| has_header | Field decides if first row should be header or entitiy.         |
| separator | Define what character is used to separete the data in columns   |

```json
"sourceConfig" : {
    "encoding": "csv",
    "columns" : ["id", "name", "age", "worksfor"],
    "has_header": true, 
    "column_separator": ","
}
```

#### incoming_mapping_config

The `incoming_mapping_config` is a JSON Object and used to provide information about how to map incoming data from the Entity to the underlying item type. The incoming mapping config is defined as follows:

| JSON Field        | Description                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------- |
| map_named         | If true, then try and lookup entity properties based on the item property name and the BaseURI prefix |
| property_mappings | An array of EntityToItemPropertyMapping objects                                                       |
| base_uri          | The BaseURI prefix                                                                                    |
| custom            | A map of custom config keys and values                                                                |

The EntityToItemPropertyMapping is defined as follows:

| JSON Field       | Description                                                            |
| ---------------- | ---------------------------------------------------------------------- |
| custom           | A map of custom config keys and values                                 |
| required         | Indicates whether the field is required                                |
| entity_property  | The entity property to map, a URL                                      |
| property         | The property to map                                                    |
| datatype         | The data type of the property                                          |
| is_reference     | Indicates whether the property is a reference                          |
| is_identity      | Indicates whether the property is an identity                          |
| is_deleted       | Indicates whether the property marks the deleted state of the entity   |
| is_recorded      | Indicates whether the property determines the entities recorded time   |
| default_value    | The default value for the property if property not found on the entity |
| strip_ref_prefix | Indicates whether to strip reference value prefixes                    |

#### outgoing_mapping_config

The `outgoing_mapping_config` is a JSON Object and used to provide information about how to map outgoing data from the underlying item type to the Entity. The outgoing mapping config is defined as follows:

| JSON Field        | Description                                                                |
|-------------------|----------------------------------------------------------------------------|
| base_uri          | Used when mapping all properties                                           |
| constructions     | An array of PropertyConstructor objects                                    |
| property_mappings | An array of ItemToEntityPropertyMapping objects                            |
| map_all           | If true, all properties are mapped                                         |
| custom            | A map of custom config keys and values                                     |
| default_type      | optional: if no rdf type is mapped then the value of this property is used |

Outgoing mappings define optional constructions and mappings. Constructions are functions that can create new properties
before any mapping is applied. This can be used, for example, to concatenate multiple properties into a single property.
A constructor is defined as follows:

| JSON Field | Description                             |
| ---------- | --------------------------------------- |
| property   | The property name to construct          |
| operation  | The operation to perform                |
| args       | An array of arguments for the operation |

The following operations are supported:

| Function Name | Arguments        | Description                                                        |
| ------------- | ---------------- | ------------------------------------------------------------------ |
| regex         | arg1, arg2       | Matches a regular expression pattern arg2 in arg1                  |
| slice         | arg1, arg2, arg3 | Extracts a substring from arg1 starting at arg2 and ending at arg3 |
| tolower       | arg1             | Converts arg1 to lowercase                                         |
| toupper       | arg1             | Converts arg1 to uppercase                                         |
| trim          | arg1             | Removes leading and trailing white spaces from arg1                |
| replace       | arg1, arg2, arg3 | Replaces all occurrences of arg2 with arg3 in arg1                 |
| split         | arg1, arg2       | Splits arg1 into an array using arg2 as the delimiter              |
| concat        | arg1, arg2       | Concatenates arg1 and arg2                                         |

Here is a sample constructor definition:

```json
{
  "property": "full_name",
  "operation": "concat",
  "args": ["firstName", "lastName"]
}
```

Constructions override existing properties if property names are the same.

When multiple constructions are defined, they are applied in the order they appear in configuration.

Newly constructed properties can also be used as input property in succeeding constructions. This way multiple
constructions can be composed into complex transformations.

The following example uses these construction semantics to combine multiple constructors and create a formatted
full name property

```json
[
  {
    "property": "separator",
    "operation": "literal",
    "args": [" "]
  },
  {
    "property": "prefixedLastName",
    "operation": "concat",
    "args": ["separator", "lastName"]
  },
  {
    "property": "fullName",
    "operation": "concat",
    "args": ["firstName", "prefixedLastName"]
  }
]
```

After any constructors the mappings are applied, they are defined as follows:

| Field Name        | Description                                                    |
| ----------------- |----------------------------------------------------------------|
| custom            | A map of custom configuration keys and values                  |
| required          | Indicates whether the field is required                        |
| entity_property   | The entity property to which the item property maps            |
| property          | The item property being mapped                                 |
| datatype          | The data type of the mapped property, optional. See list below |
| is_reference      | Indicates whether the property is a reference                  |
| uri_value_pattern | The URL value pattern                                          |
| is_identity       | Indicates whether the property is an identity                  |
| default_value     | The default value for the property                             |
| is_deleted        | Let the property contain the entities deleted state            |
| is_recorded       | Write the entity recorded time to this property                |

**Datatypes**
Datatypes are based on the [RDF datatypes](https://www.w3.org/TR/xmlschema-2/#built-in-primitive-datatypes) and can be used to cast the value of the property to the correct type. The following datatypes are supported: 

| Datatype | Golang type |
|----------|-------------|
| string   | string      |
| int      | int32       |
| long     | int64       |
| float    | float32     |
| double   | float64     |
| boolean  | bool        |



## The Mapper

The mapper is used to convert between the Entity Graph Data Model and the underlying data structures. The concept is that the mapper can be used in any data layer implementation even of the service hosting is not used. This helps to standardise the way mappings are defined across many different kinds of data layers.

An example of the mapper in use is shown below:

```go
outgoingConfig := &OutgoingMappingConfig{}
outgoingConfig.PropertyMappings = make([]*ItemToEntityPropertyMapping, 0)

outgoingConfig.PropertyMappings = append(outgoingConfig.PropertyMappings,
    &ItemToEntityPropertyMapping{
        Property:       "name",
        EntityProperty: "http://data.example.com/name",
    }, &ItemToEntityPropertyMapping{
        Property:        "id",
        IsIdentity:      true,
        URIValuePattern: "http://data.example.com/{value}",
})

// make the item
item := &InMemoryItem{properties: map[string]any{}, propertyNames: []string{}}
item.SetValue("name", "homer")
item.SetValue("id", "1")

mapper := NewMapper(logger, nil, outgoingConfig)

entity := egdm.NewEntity()
err := mapper.MapItemToEntity(item, entity)
if err != nil {
    t.Error(err)
}
```

## The Encoder

The encoder is used to encode or decode incoming or outgoing data between UDA and the format used in the source we read from or the sink we write to. Example CSV-files, parquet-files or fixed-length-files. The encoder uses the `sourceConfig` JSON object to determine how to encode or decode. 

Example of different `sourceConfig` with descriptions below

### FlatFile-config

The options for the flatfile-fields-config are:

| Field Name        | Description                                         |
| ----------------- | --------------------------------------------------- |
| name              | Name of the field/column you are reading            |
| length            | Character length of the field as an integer         |
| ignore            | Boolean that determines if the field is ignored     |
| number_pad        | if field is a number and should be padded with zeros|

The ignore field can also be used to start reading the file from an offset from start. 
The order of the fields in the array below is the order which the encoder assumes they arrive, it's important that to be aware of.

```json
"sourceConfig":{
    "encoding":"flatfile",
    "fields":[
        {
            "name":"id",
            "length":10,
            "ignore": false,
            "number_pad":false
        },
        {
            "name":"foo",
            "length":3,
            "ignore": false,
            "number_pad":false
        },
        {
            "name": "bar",
            "length":4,
            "ignore":false,
            "number_pad":false
        },
                {
            "name": "irrelevant_field",
            "length":12,
            "ignore":true,
            "number_pad":false
        }

    ]
}
```

### CSV-config

The options for the CSV-config are:

| Field Name        | Description                                         |
| ----------------- | --------------------------------------------------- |
| encoding          | What encoding method should be used                 |
| columns           | name and order of columns in the file               |
| separator         | What character separates the columns in this file   |
| has_header        | if file has a header                                |
| validate_fields   | if fields should be validated(could stop reading)   |

The order of the columns in the array below is the order which the encoder assumes they arrive, it's important that to be aware of.

```json
"sourceConfig":{
    "encoding":"csv",
    "columns":[
        "id",
        "foo",
        "bar"
    ],
    "separator":",",
    "has_header": true,
    "validateFields":false

}
```

### Parquet-config

The options for the Parquet-config are:

| Field Name        | Description                                         |
| ----------------- | --------------------------------------------------- |
| encoding          | What encoding method should be used                 |
| schema            | defines the schema of the parquet-file to be used   |
| flush_threshold   | Amount, in bytes, that will be written each pass    |
| ignore_columns    | which columns to not read from the file             |


The schema-parser reads a string looking like the one below. There is work in progress to be able to define the schema in JSON and parse it on demand to the required string.

```json
"sourceConfig":{
    "encoding":"parquet",
    "schema": "message example { required int64 id; optional binary name (STRING); optional int64 age; optional binary worksfor (STRING)}",
    "flush_threshold":2097152,
    "ignore_columns": ["irrelevant_field", "secret_field"]

}
```
### JSON-config

```json
"sourceConfig":{
    "encoding":"json",
}
```
