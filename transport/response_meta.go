package transport

// ResponseMeta captures everything about a REST response beyond the
// decoded payload: HTTP status, KuCoin's business code/message, request
// ID (if supplied), the raw body, and rate-limit/timing headers. Every
// Executor call returns one alongside the decoded result (or an error).
//
// Docs: https://www.kucoin.com/docs-new/rate-limit
type ResponseMeta struct {
	HTTPStatus int
	Code       string
	Message    string
	RequestID  string
	RawBody    []byte

	// RateLimitLimit/Remaining/Reset are the gw-ratelimit-* response
	// headers, verbatim (empty if absent).
	RateLimitLimit     string
	RateLimitRemaining string
	RateLimitReset     string

	// InTime/OutTime are the x-in-time/x-out-time response headers,
	// verbatim (empty if absent; only present when EnableNS is set).
	InTime  string
	OutTime string

	// Attempts is how many HTTP attempts were made, including retries.
	Attempts int
}
