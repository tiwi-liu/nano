package session

import "testing"

func TestRouterDeleteAddress(t *testing.T) {
	r := newRouter()
	r.Bind("UserService", "127.0.0.1:1001")
	r.Bind("WalletService", "127.0.0.1:1001")
	r.Bind("MatchService", "127.0.0.1:1002")

	r.DeleteAddress("127.0.0.1:1001")

	if _, found := r.Find("UserService"); found {
		t.Fatal("UserService route should be deleted")
	}
	if _, found := r.Find("WalletService"); found {
		t.Fatal("WalletService route should be deleted")
	}
	if got, found := r.Find("MatchService"); !found || got != "127.0.0.1:1002" {
		t.Fatalf("MatchService route = (%q, %v), want (127.0.0.1:1002, true)", got, found)
	}
}
