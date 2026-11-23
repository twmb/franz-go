package kadm

import "testing"

// Sorted previously compared topic names whenever the left ID was not less,
// even if the left ID was greater. A topic with a greater ID and a lesser
// name was then less than the other topic in both directions, and the
// returned order depended on map iteration order.
func TestTopicResponsesSortedByID(t *testing.T) {
	id1, id2 := TopicID{15: 1}, TopicID{15: 2}

	creates := CreateTopicResponses{
		"a": {Topic: "a", ID: id2},
		"b": {Topic: "b", ID: id1},
	}
	deletes := DeleteTopicResponses{
		"a": {Topic: "a", ID: id2},
		"b": {Topic: "b", ID: id1},
	}
	for range 50 { // map order is random; the sort was only sometimes wrong
		if got := creates.Sorted(); got[0].Topic != "b" || got[1].Topic != "a" {
			t.Fatalf("create: got order %s, %s; want b, a", got[0].Topic, got[1].Topic)
		}
		if got := deletes.Sorted(); got[0].Topic != "b" || got[1].Topic != "a" {
			t.Fatalf("delete: got order %s, %s; want b, a", got[0].Topic, got[1].Topic)
		}
	}
}
