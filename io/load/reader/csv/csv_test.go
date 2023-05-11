package csv

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox"
	"reflect"
	"testing"
	"time"
)

func TestCsv_Unmarshal(t *testing.T) {
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	type Boo struct {
		ID    int
		Name  string
		Price float64
		Foo   *Foo
	}

	type BooMany struct {
		ID    int
		Name  string
		Price float64
		Foo   []*Foo
	}

	type EventType struct {
		ID    int     `csvName:"Type id"`
		Price float64 `csvName:"Type price"`
		Name  string  `csvName:"Type name"`
	}

	type Event struct {
		ID        int
		Name      string
		Price     float64
		EventType *EventType
	}

	testCases := []struct {
		description string
		rType       reflect.Type
		input       string
		expected    string
		config      *Config
	}{
		{
			description: "basic",
			input: `ID,Name,Price
1,"foo",125.5`,
			rType:    reflect.TypeOf(Foo{}),
			expected: `[{"ID":1,"Name":"foo","Price":125.5}]`,
		},
		{
			description: "multiple rows",
			input: `ID,Name,Price
1,"foo",125.5
2,"boo",250`,
			rType:    reflect.TypeOf(Foo{}),
			expected: `[{"ID":1,"Name":"foo","Price":125.5}, {"ID": 2, "Name": "boo", "Price": 250}]`,
		},
		{
			description: "one to one relation",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5`,
			rType:    reflect.TypeOf(Boo{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":{"ID":10,"Name":"Foo","Price":125.5}}]`,
		},
		{
			description: "one to many relations",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5`,
			rType:    reflect.TypeOf(BooMany{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":[{"ID":10,"Name":"Foo","Price":125.5}]}]`,
		},
		{
			description: "one to many, multiple rows",
			input: `ID,Name,Price,Foo.ID,Foo.Name,Foo.Price
1,"Boo",250,10,"Foo",125.5
2,"Boo - 2",50,20,"Foo - 2",300`,
			rType:    reflect.TypeOf(BooMany{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"Foo":[{"ID":10,"Name":"Foo","Price":125.5}]},{"ID":2,"Name":"Boo - 2","Price":50,"Foo":[{"ID":20,"Name":"Foo - 2","Price":300}]}]`,
		},
		{
			description: "one to one, custom names",
			input: `ID,Name,Price,Type id,Type name,Type price
1,"Boo",250,10,"Foo",125.5
2,"Boo - 2",50,20,"Foo - 2",300`,
			rType:    reflect.TypeOf(Event{}),
			expected: `[{"ID":1,"Name":"Boo","Price":250,"EventType":{"ID":10,"Price":125.5,"Name":"Foo"}},{"ID":2,"Name":"Boo - 2","Price":50,"EventType":{"ID":20,"Price":300,"Name":"Foo - 2"}}]`,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		marshaller, err := NewMarshaller(testCase.rType, testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		dest := reflect.New(reflect.SliceOf(testCase.rType)).Interface()
		if !assert.Nil(t, marshaller.Unmarshal([]byte(testCase.input), dest), testCase.description) {
			continue
		}

		if !assertly.AssertValues(t, testCase.expected, dest) {
			toolbox.Dump(dest)
		}
	}
}

func TestCsv_Marshal(t *testing.T) {
	type Foo struct {
		ID    int
		Name  string
		Price float64
	}

	type Boo struct {
		ID   int
		Name string
		Foo  *Foo
	}

	type BooSlice struct {
		ID   int
		Name string
		Foos []*Foo
	}

	type multiSlices struct {
		ID        int
		Foos      []*Foo
		Name      string
		BooSlices []*BooSlice
		Boo       *Boo
	}

	type FooWithTime struct {
		ID      int
		Time    time.Time
		TimePtr *time.Time
	}

	testCases := []struct {
		description   string
		rType         reflect.Type
		input         interface{}
		expected      string
		config        *Config
		depthsConfigs []*Config
	}{
		{
			description: "basic",
			input: []Foo{
				{
					ID:    1,
					Name:  "Foo - 1",
					Price: 125.5,
				},
			},
			rType: reflect.TypeOf(Foo{}),
			expected: `"ID","Name","Price"
1,"Foo - 1",125.5`,
		},
		{
			description: "basic with config for float64 and float32 with prec 64",
			rType:       reflect.TypeOf(Foo{}),
			input: []Foo{
				{
					ID:    1,
					Name:  "Foo - 1",
					Price: 125.5,
				},
			},
			expected: `"ID","Name","Price"
1,"Foo - 1",125.5000000000000000000000000000000000000000000000000000000000000000`,
			config: &Config{
				StringifierConfig: io.StringifierConfig{
					Fields:     nil,
					CaseFormat: 0,
					StringifierFloat32Config: io.StringifierFloat32Config{
						Precision: "64",
					},
					StringifierFloat64Config: io.StringifierFloat64Config{
						Precision: "64",
					},
				},
			},
		},
		{
			description: "ptr",
			input: []*Foo{
				{
					ID:    1,
					Name:  "Foo - 1",
					Price: 125.5,
				},
			},
			rType: reflect.TypeOf(&Foo{}),
			expected: `"ID","Name","Price"
1,"Foo - 1",125.5`,
		},
		{
			description: "one to one",
			input: []*Boo{
				{
					ID:   1,
					Name: "Boo",
					Foo: &Foo{
						ID:    2,
						Name:  "Foo",
						Price: 125,
					},
				},
			},
			rType: reflect.TypeOf(&Boo{}),
			expected: `"ID","Name","Foo.ID","Foo.Name","Foo.Price"
1,"Boo",2,"Foo",125`,
		},
		{
			description: "nulls",
			input: []*Boo{
				{
					ID:   1,
					Name: "Boo",
				},
			},
			rType: reflect.TypeOf(&Boo{}),
			expected: `"ID","Name","Foo.ID","Foo.Name","Foo.Price"
1,"Boo",null,null,null`,
		},
		{
			description: "one to many",
			input: []*BooSlice{
				{
					ID:   1,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    2,
							Name:  "Foo - 1",
							Price: 125,
						},
						{
							ID:    3,
							Name:  "Foo - 2",
							Price: 250,
						},
					},
				},
			},
			rType: reflect.TypeOf(&BooSlice{}),
			expected: `"ID","Name","Foos.ID","Foos.Name","Foos.Price"
1,"Boo",2,"Foo - 1",125
1,"Boo",3,"Foo - 2",250`,
		},
		{
			description: "multi slices",
			input: []*multiSlices{
				{
					ID:   1,
					Name: "multiSlice with foos",
					Foos: []*Foo{
						{
							ID:    2,
							Name:  "Foo - 1",
							Price: 125,
						},
						{
							ID:    3,
							Name:  "Foo - 2",
							Price: 250,
						},
						{
							ID:    567,
							Name:  "Foo - 567",
							Price: 12345,
						},
						{
							ID:   987,
							Name: "Foo - 987",
						},
					},
					BooSlices: []*BooSlice{
						{
							ID:   123,
							Name: "boo - 123",
							Foos: []*Foo{
								{
									ID:   234,
									Name: "foo - 234",
								},
								{
									ID:   345,
									Name: "foo - 345",
								},
							},
						},
						{
							ID:   2345,
							Name: "boo - 2345",
							Foos: []*Foo{
								{
									ID:   2346,
									Name: "foo - 2346",
								},
								{
									ID:   2347,
									Name: "foo - 2347",
								},
							},
						},
					},
				},
				{
					ID:   2,
					Name: "multiSlice without foos",
					Boo: &Boo{
						ID:   4,
						Name: "Boo - name",
						Foo:  nil,
					},
					BooSlices: []*BooSlice{
						{
							ID:   5,
							Name: "Boo slice - name",
							Foos: []*Foo{
								{
									ID:    6,
									Name:  "Foo under Boo slice - 1",
									Price: 567,
								},
								{
									ID:    7,
									Name:  "Foo under Boo slice - 2",
									Price: 567,
								},
							},
						},
					},
				},
			},
			rType: reflect.TypeOf(&multiSlices{}),
			expected: `"ID","Name","Foos.ID","Foos.Name","Foos.Price","BooSlices.ID","BooSlices.Name","BooSlices.Foos.ID","BooSlices.Foos.Name","BooSlices.Foos.Price","Boo.ID","Boo.Name","Boo.Foo.ID","Boo.Foo.Name","Boo.Foo.Price"
1,"multiSlice with foos",2,"Foo - 1",125,123,"boo - 123",234,"foo - 234",0,null,null,null,null,null
1,"multiSlice with foos",3,"Foo - 2",250,123,"boo - 123",234,"foo - 234",0,null,null,null,null,null
1,"multiSlice with foos",567,"Foo - 567",12345,123,"boo - 123",234,"foo - 234",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,123,"boo - 123",234,"foo - 234",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,123,"boo - 123",345,"foo - 345",0,null,null,null,null,null
1,"multiSlice with foos",3,"Foo - 2",250,123,"boo - 123",345,"foo - 345",0,null,null,null,null,null
1,"multiSlice with foos",567,"Foo - 567",12345,123,"boo - 123",345,"foo - 345",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,123,"boo - 123",345,"foo - 345",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,2345,"boo - 2345",2346,"foo - 2346",0,null,null,null,null,null
1,"multiSlice with foos",3,"Foo - 2",250,2345,"boo - 2345",2346,"foo - 2346",0,null,null,null,null,null
1,"multiSlice with foos",567,"Foo - 567",12345,2345,"boo - 2345",2346,"foo - 2346",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,2345,"boo - 2345",2346,"foo - 2346",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,2345,"boo - 2345",2347,"foo - 2347",0,null,null,null,null,null
1,"multiSlice with foos",3,"Foo - 2",250,2345,"boo - 2345",2347,"foo - 2347",0,null,null,null,null,null
1,"multiSlice with foos",567,"Foo - 567",12345,2345,"boo - 2345",2347,"foo - 2347",0,null,null,null,null,null
1,"multiSlice with foos",987,"Foo - 987",0,2345,"boo - 2345",2347,"foo - 2347",0,null,null,null,null,null
2,"multiSlice without foos",null,null,null,5,"Boo slice - name",6,"Foo under Boo slice - 1",567,4,"Boo - name",null,null,null
2,"multiSlice without foos",null,null,null,5,"Boo slice - name",7,"Foo under Boo slice - 2",567,4,"Boo - name",null,null,null`,
		},
		{
			description: "depth configs",
			input: []*BooSlice{
				{
					ID:   1,
					Name: "Boo",
					Foos: []*Foo{
						{
							ID:    2,
							Name:  "Foo - 1",
							Price: 125,
						},
						{
							ID:    3,
							Name:  "Foo - 2",
							Price: 250,
						},
					},
				},
				{
					ID:   4,
					Name: "Boo - 4",
					Foos: []*Foo{
						{
							ID:    5,
							Name:  "Foo - 5",
							Price: 125,
						},
						{
							ID:    6,
							Name:  "Foo - 6",
							Price: 250,
						},
					},
				},
			},
			rType: reflect.TypeOf(&BooSlice{}),
			expected: `"ID","Name","Foos"
1,"Boo",'Foos.ID'#'Foos.Name'#'Foos.Price'|2#'Foo - 1'#125|3#'Foo - 2'#250
4,"Boo - 4",'Foos.ID'#'Foos.Name'#'Foos.Price'|5#'Foo - 5'#125|6#'Foo - 6'#250`,
			depthsConfigs: []*Config{
				{
					ObjectSeparator: "|",
					EscapeBy:        "/",
					FieldSeparator:  "#",
					EncloseBy:       "'",
				},
			},
		},
		{
			description: "times",
			input: []*FooWithTime{
				{
					ID:      1,
					Time:    newTime("2019-01-02"),
					TimePtr: newTimePtr("2020-01-02"),
				},
				{
					ID:   2,
					Time: newTime("2020-04-04"),
				},
			},
			rType: reflect.TypeOf(&FooWithTime{}),
			expected: `"ID","Time","TimePtr"
1,"2019-01-02T00:00:00Z","2020-01-02T00:00:00Z"
2,"2020-04-04T00:00:00Z",null`,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		marshaller, err := NewMarshaller(testCase.rType, testCase.config)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		marshal, err := marshaller.Marshal(testCase.input, testCase.depthsConfigs)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if !assertly.AssertValues(t, testCase.expected, marshal) {
			fmt.Println(string(marshal))
		}
	}
}

func newTime(date string) time.Time {
	parse, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return parse
}

func newTimePtr(date string) *time.Time {
	aTime := newTime(date)
	return &aTime
}
