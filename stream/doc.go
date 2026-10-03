// Package stream defines the connection-lifecycle vocabulary shared by every
// KuCoin WebSocket client in this module: connection State and lifecycle
// Event values, typed errors, the generic Subscription handle that streaming
// packages return, the Handler interface for custom frame processing, and the
// Config used to tune reconnect, heartbeat and back-pressure behaviour.
//
// Applications normally never construct these types. They obtain a typed
// subscription from a streaming package, for example:
//
//	sub, err := session.SubscribeTickerV2(ctx, "XBTUSDTM")
//	if err != nil { ... }
//	defer sub.Close()
//	for tick := range sub.C() {
//		fmt.Println(tick.Symbol, tick.BestBidPrice, tick.BestAskPrice)
//	}
//	if err := sub.Err(); err != nil { ... } // why the stream ended
//
// # Delivery guarantees
//
// Updates on one Subscription are delivered in the order KuCoin sent them.
// Each subscription owns a bounded queue; when the consumer cannot keep up,
// the OverflowPolicy decides what happens (the default drops the oldest
// queued update and counts it in Subscription.Dropped). A connection loss is
// reported through lifecycle events and, for sequenced streams such as order
// books, triggers an automatic resynchronisation.
//
// # Typed errors
//
// Failures are reported as values that work with errors.Is and errors.As: the
// sentinel errors in this package (ErrClosed, ErrNotConnected, ErrPingTimeout,
// ErrSubscriptionLimit, ...) and *ServerError for rejections reported by
// KuCoin itself.
package stream
