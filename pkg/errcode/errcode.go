package errcode

// Code is a system-level response code encoded in the response header.
// Business errors belong in the response body and must not be added here.
type Code uint8

// System response codes. The wire protocol reserves four bits, so valid codes
// must remain in the range [0, 15].
const (
	CodeOk                 Code = iota
	CodeNotContent              = 1
	CodeBadRequest              = 2
	CodePermissionDenied        = 3
	CodeMethodNotAllowed        = 4
	CodeRequestTimeout          = 5
	CodeInternalErr             = 6
	CodeServiceNotFound         = 7
	CodeMethodNotFound          = 8
	CodeNetworkException        = 9
	CodeServiceUnavailable      = 10
	CodeQueueNotFund            = 11
	CodeProtoParseFail          = 12
	CodeAsyncReturn             = 13
	CodeUnknown                 = 14
	CodeServerBusy              = 15
)

func FromWire(value uint64) (Code, bool) {
	if value > uint64(CodeServerBusy) {
		return CodeUnknown, false
	}
	return Code(value), true
}
