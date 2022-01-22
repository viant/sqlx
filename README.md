# sqlx - Comprehensive SQL Extensions For Go

## Motivation

## Usage

Before using any of the CRUD operation, You have to import package with required product. All of the supported products
are specified at the `sqlx/metadata/product` package e.g. If you want to use `MySQL` database you need to import `MySQL`
product:

```go
    import _ "github.com/viant/sqlx/metadata/product/mysql"
```

You also need to import `load` package if you want to use `sqlx/io/load` e.g. If you want to load data into `MySQL`
database, you need to import `MySQL` load package:

```go
    import _ "github.com/viant/sqlx/metadata/product/mysql/load"
```

## Contribution

## License

## Author

