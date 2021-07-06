# Database vendor customization


## Introduction

Product package defines packages for various database products providing schema metadata information.
To unify queries across various vendor this library uses [metadata.info.Kind](../../metadata/info/kind.go) 
and corresponding [metadata.sink](../../metadata/sink), storing supper set information across all vendors.
Each kind requires fixed number of criteria parameters,  for example to query table metadata information, catalog and schema is required.
Inspect [metadata.info.Kind](../../metadata/info/kind.go#L86) for more details.
To define metadata information, register [Query](../../metadata/info/query.go), with product including min supported version,
actual SQL, and criteria parameters matching exactly defining kind. 
Criteria allow dynamic SQL building depending on criterion value which can define the following:
- name of the aliased column used in WHERE clause
- empty string in case vendor does not support/ignore specific criteria (i.e catalog)
- '?' in case SQL embedded placeholder that binds with query parameter
- '%' in case SQL embed placeholder that needs be substituted while building SQL 


