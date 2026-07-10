package errcode

import "testing"

func TestSystemCodesMatchFourBitWireValues(t *testing.T) {
	want := []Code{
		CodeOk,
		CodeNotContent,
		CodeBadRequest,
		CodePermissionDenied,
		CodeMethodNotAllowed,
		CodeRequestTimeout,
		CodeInternalErr,
		CodeServiceNotFound,
		CodeMethodNotFound,
		CodeNetworkException,
		CodeServiceUnavailable,
		CodeQueueNotFund,
		CodeProtoParseFail,
		CodeAsyncReturn,
		CodeUnknown,
		CodeServerBusy,
	}
	for value, code := range want {
		if code != Code(value) {
			t.Fatalf("code at index %d = %d", value, code)
		}
	}
}

func TestFromWireRejectsValuesOutsideHeaderRange(t *testing.T) {
	if code, ok := FromWire(16); ok || code != CodeUnknown {
		t.Fatalf("FromWire(16) = (%d, %v), want (CodeUnknown, false)", code, ok)
	}
}
