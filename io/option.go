package io

//Option represents generic option
type Option interface{}

//Options represents generic options
type Options []Option

//Tag returns annotation tag, default sqlx
func (o Options) Tag() string {
	if len(o) == 0 {
		return tagSqlx
	}
	for _, candidate := range o {
		if tagOpt, ok := candidate.(TagOption); ok {
			return tagOpt.Tag
		}
	}
	return tagSqlx
}

//TagOption represent a annotation tag
type TagOption struct {
	Tag string
}
