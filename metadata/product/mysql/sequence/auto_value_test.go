package sequence

import "testing"

func TestAutoIncrementMetadataOption(t *testing.T) {
	for _, tc := range []struct {
		ddl, mode string
		want      uint64
	}{
		{"CREATE TABLE `t` (`id` bigint NOT NULL AUTO_INCREMENT,`note` text COMMENT 'AUTO_INCREMENT=999') ENGINE=InnoDB AUTO_INCREMENT=42 COMMENT='AUTO_INCREMENT=888'", "", 42},
		{"CREATE TABLE `AUTO_INCREMENT=99` (`id` bigint AUTO_INCREMENT) ENGINE=InnoDB COMMENT='AUTO_INCREMENT=777'", "", 1},
		{"CREATE TABLE t (id bigint AUTO_INCREMENT,note text COMMENT 'x'' AUTO_INCREMENT=8') ENGINE=InnoDB AUTO_INCREMENT=43", "NO_BACKSLASH_ESCAPES", 43},
		{`CREATE TABLE t (id bigint AUTO_INCREMENT,note text COMMENT 'x\' AUTO_INCREMENT=8') ENGINE=InnoDB AUTO_INCREMENT=44`, "", 44},
		{"CREATE TABLE t (id bigint AUTO_INCREMENT) ENGINE=InnoDB AUTO_INCREMENT=18446744073709551615", "", ^uint64(0)},
	} {
		got, err := (&ReservationMetadata{}).tableAutoValue(tc.ddl, tc.mode)
		if err != nil || got != tc.want {
			t.Fatalf("metadata next=%d want=%d err=%v", got, tc.want, err)
		}
	}
}
