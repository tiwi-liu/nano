package errcode

// Code is a system-level response code encoded in the response header.
// Business errors belong in the response body and must not be added here.
type Code uint8

// System response codes. The wire protocol reserves four bits, so valid codes
// must remain in the range [0, 15].
const (
	CodeOk Code = iota
	CodeNotContent
	CodeBadRequest
	CodePermissionDenied
	CodeMethodNotAllowed
	CodeRequestTimeout
	CodeInternalErr
	CodeServiceNotFound
	CodeMethodNotFound
	CodeNetworkException
	CodeServiceUnavailable
	CodeQueueNotFund
	CodeProtoParseFail
	CodeAsyncReturn
	CodeUnknown
	CodeServerBusy
)

func FromWire(value uint64) (Code, bool) {
	if value > uint64(CodeServerBusy) {
		return CodeUnknown, false
	}
	return Code(value), true
}
