package aerospike

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTryOrderedSQLQuotedProjection(t *testing.T) {
	for _, tc := range []struct{ name, projection, order, outer string }{
		{"unquoted control", "t.advertiser_time, t.audience_id", "advertiser_time", "advertiser_time"},
		{"quoted projection", "t.`advertiser_time`, t.`audience_id`", "advertiser_time", "`advertiser_time`"},
		{"quoted order", "t.advertiser_time, t.audience_id", "`advertiser_time` DESC", "advertiser_time DESC"},
		{"quoted qualifier", "`t`.`advertiser_time`, t.audience_id", "t.advertiser_time ASC", "`advertiser_time` ASC"},
		{"qualified order", "advertiser_time, audience_id", "t.`advertiser_time` DESC", "advertiser_time DESC"},
		{"alias", "t.advertiser_time AS `ordered_at`, t.audience_id", "ordered_at DESC", "`ordered_at` DESC"},
		{"source through alias", "t.`advertiser_time` AS ordered_at, t.audience_id", "t.advertiser_time DESC", "ordered_at DESC"},
		{"multiple terms", "t.`advertiser_time`, t.audience_id, t.metric_name AS `label`", "advertiser_time DESC, `label` ASC", "`advertiser_time` DESC, `label` ASC"},
		{"case and quoting", "t.`Advertiser_Time`, t.audience_id", "advertiser_time DESC", "`Advertiser_Time` DESC"},
		{"escaped quote", "t.`event``time` AS `ordered``at`, t.audience_id", "t.`event``time` DESC", "`ordered``at` DESC"},
		{"bracket alias", "t.advertiser_time AS [ordered_at], t.audience_id", "ordered_at DESC", "[ordered_at] DESC"},
		{"double quoted alias", "t.advertiser_time AS \"ordered at\", t.audience_id", "`ordered at` DESC", "\"ordered at\" DESC"},
		{"quoted dotted leaf", "t.`event.time` AS ordered_at, t.audience_id", "t.`event.time` DESC", "ordered_at DESC"},
		{"literal dot versus qualification", "t.`event.time` AS literal, t.time AS plain, t.audience_id", "t.`event.time`, t.time DESC", "literal, plain DESC"},
		{"qualified duplicates with unique aliases", "a.`event_time` AS first_time, b.event_time AS second_time", "a.event_time, b.`event_time` DESC", "first_time, second_time DESC"},
		{"reserved output", "t.advertiser_time AS `order`, t.audience_id", "`order`", "`order`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := "SELECT " + tc.projection + " FROM (SELECT advertiser_time, audience_id FROM performance) t ORDER BY " + tc.order
			got, ordered := tryOrderedSQL(input, "audience_id")
			require.True(t, ordered)
			require.Equal(t, "SELECT * FROM ("+input+") AS _sqlx_warmup ORDER BY audience_id, "+tc.outer, got)
		})
	}
}

func TestTryOrderedSQLQuotedProjectionAmbiguity(t *testing.T) {
	for _, tc := range []struct{ projection, order string }{
		{"a.event_time AS first_time, b.`event_time` AS second_time", "event_time"},
		{"a.event_time, b.`event_time`", "a.event_time"},
		{"a.event_time AS ordered_at, b.event_time AS `ordered_at`", "a.event_time"},
		{"a.event_time AS x, b.other_time AS `x`", "a.event_time"},
		{"a.`event_time` AS ordered_at", "b.event_time"},
		{"a.event_time AS x, a.event_time AS y, b.event_time AS z", "a.event_time"},
		{"t.`event.time` AS ordered_at", "t.event.time"},
		{"t.event_time", "event_time, missing_column"},
	} {
		t.Run(tc.projection+"/"+tc.order, func(t *testing.T) {
			input := "SELECT audience_id, " + tc.projection + " FROM records t ORDER BY " + tc.order
			got, ordered := tryOrderedSQL(input, "audience_id")
			require.True(t, ordered)
			require.Equal(t, "SELECT * FROM ("+input+") AS _sqlx_warmup ORDER BY audience_id", got)
		})
	}
}

func TestTryOrderedSQLQuotedFirstOrder(t *testing.T) {
	for _, order := range []string{"t.`audience_id`", "`t`.`audience_id` DESC", "`audience_id`", "[audience_id]"} {
		input := "SELECT t.`audience_id`, t.`event_time` FROM records t ORDER BY " + order
		got, ordered := tryOrderedSQL(input, "audience_id")
		require.True(t, ordered)
		require.Equal(t, input, got)
	}
	input := "SELECT `t.audience_id`, audience_id FROM records ORDER BY `t.audience_id`"
	got, ordered := tryOrderedSQL(input, "audience_id")
	require.True(t, ordered)
	require.Equal(t, "SELECT * FROM ("+input+") AS _sqlx_warmup ORDER BY audience_id, `t.audience_id`", got)
	for _, input := range []string{
		"SELECT * FROM records ORDER BY audience_id, event_time",
		"SELECT COUNT(*) AS audience_id FROM records GROUP BY event_time ORDER BY audience_id",
	} {
		got, ordered := tryOrderedSQL(input, "audience_id")
		require.True(t, ordered)
		require.Equal(t, input, got)
	}
}

func TestTryOrderedSQLConflictingFirstOrder(t *testing.T) {
	input := "SELECT a.id AS other_id, b.`id` AS id FROM records a JOIN records b ON a.id = b.id ORDER BY a.id"
	got, ordered := tryOrderedSQL(input, "id")
	require.True(t, ordered)
	require.Equal(t, "SELECT * FROM ("+input+") AS _sqlx_warmup ORDER BY id, other_id", got)
}

func TestTryOrderedSQLQuotedTimelineRows(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE performance (audience_id INTEGER, advertiser_time INTEGER)")
	require.NoError(t, err)
	for hour := 23; hour >= 0; hour-- {
		for _, audience := range []int{2, 1} {
			_, err = db.Exec("INSERT INTO performance VALUES (?, ?)", audience, hour)
			require.NoError(t, err)
		}
	}
	for _, direction := range []string{"ASC", "DESC"} {
		input := "SELECT t.`advertiser_time`, t.`audience_id` FROM (SELECT advertiser_time, audience_id FROM performance) t ORDER BY advertiser_time " + direction
		ordered, ok := tryOrderedSQL(input, "audience_id")
		require.True(t, ok)
		rows, err := db.Query(ordered)
		require.NoError(t, err)
		var actual []string
		for rows.Next() {
			var hour, audience int
			require.NoError(t, rows.Scan(&hour, &audience))
			actual = append(actual, fmt.Sprintf("%d:%d", audience, hour))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		var expected []string
		for audience := 1; audience <= 2; audience++ {
			for position := 0; position < 24; position++ {
				hour := position
				if direction == "DESC" {
					hour = 23 - position
				}
				expected = append(expected, fmt.Sprintf("%d:%d", audience, hour))
			}
		}
		require.Equal(t, expected, actual)
	}
}
