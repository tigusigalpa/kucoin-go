package streaming

// Frames below are the worked examples of KuCoin's official UTA WebSocket v2
// documentation (https://www.kucoin.com/docs-new/ page ids in the comments), with
// the documentation's inline comments removed so they are valid JSON. Frames whose
// name ends in "Variant" have the same shape with values chosen, and keys ordered,
// so that a wrongly mapped field cannot go unnoticed; frames marked "live" were
// captured from the public gateway on 2026-10-03.
const (
	// 3470355 Ticker
	fxTickerSpot    = `{"T":"ticker.SPOT","P":1768206966101166007,"d":{"A":"0.97675941","B":"0.02052839","E":25958853459,"M":1768206966096000000,"S":"BUY","a":"90968.2","b":"90968.1","l":"90968.2","q":"0.00109929","s":"BTC-USDT"}}`
	fxTickerFutures = `{"T":"ticker.FUTURES","P":1768218267869446269,"d":{"a":"90580.5","A":"36","q":"3","b":"90580.4","B":"4852","s":"XBTUSDTM","S":"buy","E":1905974001288,"l":"90580.5","M":1768218267868000000}}`
	// variant: upper-case keys before their lower-case twins, every value distinct
	fxTickerVariant = `{"T":"ticker.FUTURES","P":5,"d":{"S":"sell","B":"7","A":"9","s":"ETHUSDTM","b":"2000.5","a":"2000.6","E":42,"l":"2000.55","q":"1.5","M":6}}`

	// 3470356 Kline
	fxKlineSpot    = `{"T":"kline.SPOT","P":1776090720846219590,"d":{"o":"71725.1","c":"71728.1","l":"71725.1","h":"71728.1","S":false,"v":"0.01504768","i":"1min","a":"1079.297822768","C":1776090780,"s":"BTC-USDT","O":1776090720}}`
	fxKlineFutures = `{"T":"kline.FUTURES","P":1768803943947934451,"d":{"a":"67919.2142","s":"XBTUSDTM","C":1768803960,"c":"92551.6","S":true,"v":"734","h":"92551.7","i":"1min","l":"92505.1","O":1768803900,"o":"92505.1"}}`
	// variant: distinct open/close/high/low and upper-case keys first
	fxKlineVariant = `{"T":"kline.SPOT","P":9,"d":{"S":true,"O":100,"C":160,"s":"ETH-USDT","o":"1","c":"2","h":"3","l":"0.5","i":"5min","v":"11","a":"12"}}`

	// 3470359 Trade
	fxTradeSpot    = `{"T":"trade.SPOT","P":1768802538491160904,"d":{"E":20631804219899904,"M":1768802538480000000,"S":"sell","p":"92525.6","q":"0.00008036","s":"BTC-USDT","ti":"20631804219899904"}}`
	fxTradeFutures = `{"T":"trade.FUTURES","P":1768218157098477768,"d":{"p":"90551.8","q":"12","s":"XBTUSDTM","S":"buy","E":1905973690439,"ti":"1905973690439","rpi":false,"M":1768218157097000000}}`
	// variant: an RPI trade whose trade ID is a JSON number
	fxTradeVariant = `{"T":"trade.FUTURES","P":8,"d":{"S":"SELL","s":"ETHUSDTM","ti":777,"rpi":true,"E":3,"p":"2000.1","q":"4","M":2}}`

	// 3470354 Orderbook: the documented examples of every depth
	fxObuSpotBBO          = `{"T":"obu.SPOT","dp":"1","t":"snapshot","P":1768217874990007701,"d":{"C":25984468414,"M":1768217874986000000,"O":25984468414,"a":[["90701.2","0.5771583"]],"b":[["90701.1","0.13918404"]],"s":"BTC-USDT"}}`
	fxObuSpotIncrement    = `{"T":"obu.SPOT","dp":"increment","t":"delta","P":1768217909684719896,"d":{"C":25984544840,"M":1768217909683000000,"O":25984544839,"a":[],"b":[["1","12996.24994153"]],"s":"BTC-USDT"}}`
	fxObuSpot10ms         = `{"T":"obu.SPOT","dp":"increment@10ms","t":"delta","P":1781672136338555550,"d":{"C":33629695393,"M":1781672136333000000,"O":33629695388,"a":[],"b":[["65928.9","0"],["65946.6","0"],["65947","0.06185847"]],"s":"BTC-USDT"}}`
	fxObuFuturesBBO       = `{"T":"obu.FUTURES","dp":"1","t":"snapshot","P":1768217994549513415,"d":{"a":[["90629.2","2236"]],"b":[["90629.1","301"]],"s":"XBTUSDTM","C":1732420529826,"M":1768217994548000000,"O":1732420529826}}`
	fxObuFuturesIncrement = `{"T":"obu.FUTURES","dp":"increment","t":"delta","P":1768218044321756061,"d":{"a":[["90601.5","0"]],"b":[],"C":1732420557695,"s":"XBTUSDTM","M":1768218044321000000,"O":1732420557695}}`
	fxObuFutures10ms      = `{"T":"obu.FUTURES","dp":"increment@10ms","t":"delta","P":1781666796660937177,"d":{"a":[],"b":[["65739.9","0"],["65739","14"]],"C":1743938538056,"s":"XBTUSDTM","M":1781666796658000000,"O":1743938538050}}`
	// the RPI example lists the envelope keys in another order than every other frame
	fxObuFuturesRPI = `{"t":"snapshot","T":"obu.FUTURES","d":{"O":1731931329201,"b":[["88862.4","891","0"]],"M":1767323800296000000,"s":"XBTUSDTM","a":[["88862.5","1645","0"]],"C":1731931329201},"P":1767323800305802104,"dp":"5"}`
	// live (abridged): a depth 5 snapshot of spot and a depth 50 snapshot of futures
	fxObuSpot5Live     = `{"T":"obu.SPOT","dp":"5","t":"snapshot","P":1790984756520149607,"d":{"C":23242136205,"M":1790984756494000000,"O":23242136205,"a":[["2669.63","5.6261598"],["2669.64","3.0470222"],["2669.65","0.002"],["2669.67","2.2293225"],["2669.71","1.6184204"]],"b":[["2669.62","3.1520438"],["2669.6","0.002"],["2669.58","0.0038729"],["2669.56","0.6156576"],["2669.54","0.004"]],"s":"ETH-USDT"}}`
	fxObuFutures50Live = `{"T":"obu.FUTURES","dp":"50","t":"snapshot","P":1790984756592972774,"d":{"a":[["118.632","299"],["118.641","2"]],"b":[["118.631","293"],["118.63","100"]],"s":"SOLUSDTM","C":1739906836590,"M":1790984756579000000,"O":1739906836590}}`
	// live: an RPI snapshot with non-RPI and RPI sizes
	fxObuFuturesRPILive = `{"T":"obu.FUTURES","dp":"5","t":"snapshot","P":1790984884086435262,"d":{"a":[["84516.3","457","0"],["84516.4","0","449"]],"b":[["84516.2","178","156"],["84516.1","33","0"]],"s":"XBTUSDTM","C":1750227356351,"M":1790984884059000000,"O":1750227356351}}`

	// 3470358 Mark Price: the published example (its P is in milliseconds) and a live
	// frame of another symbol
	fxMarkPrice     = `{"T":"mark-price","P":1731899129000,"d":{"s":"XBTUSDTM","mp":"90445","ip":"90445","oi":"23445","ts":1731899128999}}`
	fxMarkPriceLive = `{"T":"mark-price","P":1790984885234256943,"d":{"s":"ETHUSDTM","mp":"84517.2","ip":"84564.02","oi":"11793380","ts":1790984885000}}`

	// 3470357 Funding Fee Rate
	fxFundingRate = `{"T":"funding-fee","P":1786809901214267234,"d":{"s":"XBTUSDTM","fr":"0.000072","ft":1786809600000,"lfr":"0.000069","nt":1786838400000,"gl":28800000,"fc":"0.003","ff":"-0.003"}}`
	// 3470412 All Funding Fee Rates
	fxFundingAll = `{"T":"funding-fee-all-symbols","P":1786809901214267234,"d":[{"s":"XBTUSDTM","fr":"0.000072","ft":1786809600000,"lfr":"0.000069","nt":1786838400000,"gl":28800000,"fc":"0.003","ff":"-0.003"},{"s":"ETHUSDTM","fr":"0.000041","ft":1786809600000,"lfr":"0.000039","nt":1786838400000,"gl":28800000,"fc":"0.003","ff":"-0.003"}]}`

	// 3470353 Call Auction Data: the example sends numbers and names the price "eq"
	// although the schema says "ep"
	fxCallAuction = `{"T":"callAuctionInfo.SPOT","t":"snapshot","P":1783434842522281439,"d":{"s":"GROVE-USDT","slp":0.05568,"blp":0.00006,"shp":0.19839,"bhp":0.19,"eq":0.05568,"es":9099.4,"ts":1783434842521}}`
	// variant: the schema's spelling with strings
	fxCallAuctionVariant = `{"T":"callAuctionInfo.SPOT","t":"snapshot","P":1783434842622281439,"d":{"s":"GROVE-USDT","ts":1783434842621,"es":"100.5","ep":"0.06","bhp":"0.2","blp":"0.0001","shp":"0.3","slp":"0.05"}}`

	// 3470346 Order: every status of the documentation. Spot market order, live and filled.
	fxOrderSpotLive   = `{"P":1770380108525898382,"T":"orderAll.UNIFIED","d":{"O":1770380108442909926,"S":"BUY","U":1770380108525522700,"f":"0","p":"","q":"0.00001","s":"BTC-USDT","t":"0","aP":"0","cR":"","cS":"0","ci":"39350f44-56ef-4b91-9788-708d9ed83e95","eT":"OPEN","fC":"USDT","fS":"0","lP":"","lR":"","lS":"0","ls":"0","oS":"USER","oT":"MARKET","oi":"409225265957388288","os":2,"pO":false,"pP":"","qU":"BASECCY","rO":false,"rS":"0.00001","tD":"","tP":"","tT":"SPOT","ti":"","lPT":"","pPT":"","stp":"","tIF":"GTC","tPT":"","toi":"","bT":""}}`
	fxOrderSpotFilled = `{"P":1770380108536802787,"T":"orderAll.UNIFIED","d":{"O":1770380108442909926,"S":"BUY","U":1770380108526000000,"f":"0.00066421","p":"","q":"0.00001","s":"BTC-USDT","t":"0","aP":"66421","cR":"","cS":"0","ci":"39350f44-56ef-4b91-9788-708d9ed83e95","eT":"FILL","fC":"USDT","fS":"0.00001","lP":"","lR":"TAKER","lS":"0.00001","ls":"66421","oS":"USER","oT":"MARKET","oi":"409225265957388288","os":3,"pO":false,"pP":"","qU":"BASECCY","rO":false,"rS":"0","tD":"","tP":"","tT":"SPOT","ti":"21105572357619712","lPT":"","pPT":"","stp":"","tIF":"GTC","tPT":"","toi":"","bT":""}}`
	// spot conditional order: not triggered (status 0) and triggered (status 1)
	fxOrderConditional = `{"P":1770380369218928660,"T":"orderAll.UNIFIED","d":{"O":1770380369215582232,"S":"BUY","U":1770380369218449310,"f":"0","p":"","q":"0.00001","s":"BTC-USDT","t":"0","aP":"0","cR":"","cS":"0","ci":"273edcef-532d-46ba-9e0c-14a8da03ca9b","eT":"OPEN","fC":"USDT","fS":"0","lP":"","lR":"","lS":"0","ls":"0","oS":"USER","oT":"MARKET","oi":"409226359718625280","os":0,"pO":false,"pP":"","qU":"BASECCY","rO":false,"rS":"0.00001","tD":"UP","tP":"66220","tT":"SPOT","ti":"","lPT":"","pPT":"","stp":"","tIF":"GTC","tPT":"TP","toi":"","bT":""}}`
	fxOrderTriggered   = `{"P":1770380370017184723,"T":"orderAll.UNIFIED","d":{"O":1770380369215582232,"S":"BUY","U":1770380370016801257,"f":"0","p":"","q":"0.00001","s":"BTC-USDT","t":"0","aP":"0","cR":"","cS":"0","ci":"273edcef-532d-46ba-9e0c-14a8da03ca9b","eT":"TRIGGER","fC":"USDT","fS":"0","lP":"","lR":"","lS":"0","ls":"0","oS":"USER","oT":"MARKET","oi":"409226359718625280","os":1,"pO":false,"pP":"","qU":"BASECCY","rO":false,"rS":"0.00001","tD":"UP","tP":"66220","tT":"SPOT","ti":"","lPT":"","pPT":"","stp":"","tIF":"GTC","tPT":"TP","toi":"","bT":""}}`
	// futures order with take-profit and stop-loss; the documentation's frame with its
	// push type changed to "order.UNIFIED", the one of a single-symbol subscription
	fxOrderTPSL = `{"P":1770348921636694650,"T":"order.UNIFIED","d":{"O":1770348921635950111,"S":"BUY","U":1770348921636284837,"f":"0","p":"","q":"1","s":"XBTUSDTM","t":"0","aP":"0","cR":"","cS":"0","ci":"8c0c0f02-0c53-4e75-92f6-4bdfacb34496","eT":"OPEN","fC":"USDT","fS":"0","lP":"64800","lR":"","lS":"0","ls":"0","oS":"USER","oT":"MARKET","oi":"409094459008032768","os":2,"pO":false,"pP":"64880","qU":"UNIT","rO":false,"rS":"1","tD":"","tP":"","tT":"FUTURES","ti":"","lPT":"TP","pPT":"TP","stp":"","tIF":"GTC","tPT":"","toi":"","bT":""}}`
	// futures reduce-only conditional order canceled by the system (status 5)
	fxOrderCanceled = `{"P":1770348960158687824,"T":"orderAll.UNIFIED","d":{"O":1770348921643622286,"S":"SELL","U":1770348960158129346,"f":"0","p":"","q":"1","s":"XBTUSDTM","t":"0","aP":"0","cR":"SYSTEM_CANCEL","cS":"1","ci":"","eT":"CANCEL","fC":"USDT","fS":"0","lP":"","lR":"","lS":"0","ls":"0","oS":"USER","oT":"MARKET","oi":"409094459027223421","os":5,"pO":false,"pP":"","qU":"UNIT","rO":true,"rS":"0","tD":"DOWN","tP":"64800","tT":"FUTURES","ti":"","lPT":"","pPT":"","stp":"","tIF":"GTC","tPT":"TP","toi":"409094459008032768","bT":""}}`
	// the "UTA - UNIFIED" example: another key order, a margin mode and a numeric cS
	fxOrderUnified = `{"T":"orderAll.UNIFIED","P":1776153224036666053,"d":{"tT":"SPOT","oi":"433439467777417216","ci":"273edcef-532d-46ba-9e0c-14a8da03ca9b","os":2,"eT":"OPEN","s":"XRP-USDT","S":"BUY","oT":"MARKET","lR":"","oS":"USER","p":"","mM":"","ti":"","q":"0.1","qU":"BASECCY","fS":"0","lS":"0","ls":"0","aP":"0","f":"0","fC":"USDT","t":"0","cR":"","cS":0,"rS":"0.1","tD":"","tP":"","tPT":"","pP":"","pPT":"","lP":"","lPT":"","toi":"","stp":"","rO":false,"tIF":"GTC","pO":false,"O":1776153224034091350,"U":1776153224036325316,"bT":""}}`
	// variants: statuses 4 and 6 (not in the documentation's examples), every value
	// distinct and carrying keys no example uses (pS, pOP, lOP); the first lists
	// lower-case keys before their upper-case twins, the second the reverse
	fxOrderPartialVariant  = `{"T":"order.UNIFIED","P":11,"d":{"s":"ETHUSDTM","ls":"2001.5","os":4,"S":"SELL","lS":"3","oS":"USER","oi":"900","ci":"c-1","eT":"MATCH","tT":"FUTURES","pS":"SHORT","oT":"LIMIT","lR":"MAKER","p":"2002","mM":"CROSS","ti":4242,"q":"10","qU":"UNIT","fS":"3","aP":"2001.2","f":"0.5","fC":"USDT","t":"0.01","cR":"","cS":"0","rS":"7","tD":"","tP":"","tPT":"","pP":"","pPT":"","pOP":"","lP":"","lPT":"","lOP":"","toi":"","stp":"CB","rO":true,"tIF":"GTT","pO":true,"O":5,"U":6,"bT":"broker"}}`
	fxOrderPartialCanceled = `{"T":"order.UNIFIED","P":12,"d":{"S":"BUY","oS":"USER","lS":"1","ls":"64750.5","os":6,"s":"XBTUSDTM","oi":"901","eT":"CANCEL","tT":"FUTURES","pS":"LONG","oT":"LIMIT","p":"64750","mM":"ISOLATED","q":"5","fS":"2","cR":"USER","cS":"3","rS":"0","pP":"65000","pPT":"MP","pOP":"65010","lP":"64000","lPT":"IP","lOP":"63990","O":7,"U":8}}`

	// 3470406 Execution: every fill type of the documentation
	fxExecutionNormal     = `{"T":"execution.UNIFIED","P":1786516889648920721,"d":{"oi":"476907831686172672","s":"XRP-USDT","S":"BUY","oT":"MARKET","p":"1.01972","q":"0.9806","ti":"22221471895799808","E":1786516889638000000,"lR":"TAKER","f":"0.000999937432","fC":"USDT","fT":"NORMAL","ci":"9b5620d9-ef1c-4ab5-89d1-717b2602d445","fP":""}}`
	fxExecutionADL        = `{"T":"execution.UNIFIED","P":1786353005793808741,"d":{"oi":"0","s":"XBTUSDTM","S":"BUY","oT":"","p":"81580.1469961881","q":"34","ti":"1785856966900","E":1786353005359000000,"lR":"","f":"0","fC":"USDT","fT":"ADL","ci":"","fP":""}}`
	fxExecutionLiquid     = `{"T":"execution.UNIFIED","P":1785393975535846294,"d":{"oi":"0","s":"XBTUSDTM","S":"BUY","oT":"","p":"63962.4799219875","q":"8","ti":"1780500473345","E":1785393975227000000,"lR":"TAKER","f":"0","fC":"USDT","fT":"LIQUID","ci":"","fP":""}}`
	fxExecutionSettlement = `{"T":"execution.UNIFIED","P":1785399944158708818,"d":{"oi":"0","s":"ETHUSDTM","S":"SELL","oT":"","p":"3800","q":"40","ti":"1780499594698","E":1785399943899000000,"lR":"TAKER","f":"0","fC":"USDT","fT":"SETTLEMENT","ci":"","fP":""}}`
	// variant: a closing fill with a closed PnL, upper-case keys first
	fxExecutionVariant = `{"T":"execution.UNIFIED","P":13,"d":{"S":"sell","oi":"55","s":"ETHUSDTM","oT":"LIMIT","p":"2000.5","q":"2","ti":9,"E":99,"lR":"MAKER","f":"0.004","fC":"USDT","fT":"NORMAL","ci":"c-9","fP":"-1.25"}}`

	// 3470348 Execution Lite: the trade ID is a number on spot and a string on futures
	fxExecutionLiteSpot    = `{"T":"execution.lite.UNIFIED","P":1774854553398639926,"d":{"E":1774854553397000000,"S":"BUY","p":"67396.6","q":"0.00001483","s":"BTC-USDT","lR":"TAKER","oT":"MARKET","oi":"427992448220876800","ti":22075878662488064}}`
	fxExecutionLiteFutures = `{"T":"execution.lite.UNIFIED","P":1774854798514737265,"d":{"p":"67391.6","q":"1","s":"XBTUSDTM","S":"BUY","ti":"1928560862264","E":1774854798514000000,"oT":"MARKET","lR":"TAKER","oi":"427993476303507456"}}`
	fxExecutionLiteMargin  = `{"T":"execution.lite.UNIFIED","P":1776239056265949041,"d":{"E":1776239056263000000,"S":"BUY","p":"1.35294","q":"0.7391","s":"XRP-USDT","lR":"TAKER","oT":"MARKET","oi":"433799474222030848","ti":20939120797435904}}`
	// variant: with the client order ID of the schema and upper-case keys first
	fxExecutionLiteVariant = `{"T":"execution.lite.UNIFIED","P":14,"d":{"S":"SELL","E":77,"ci":"c-7","s":"SOL-USDT","p":"118.7","q":"3","lR":"MAKER","oT":"LIMIT","oi":"66","ti":"88"}}`

	// 3470347 Balance
	fxBalanceUnified = `{"P":1770116995060810093,"T":"balance.UNIFIED","d":{"U":1770116995058000000,"a":"0.0000517000","b":"0.0000517000","c":"BTC","e":"0.0000517000","h":"0.0000000000","l":"0.0000000000","cS":"1"}}`
	fxBalanceFunding = `{"T":"balance.FUNDING","P":1780543242950333125,"d":{"U":"1780543242933","a":"531.42853547","b":"531.42853547","c":"USDT","h":"0"}}`
	// variant: every optional field, a null equity and the sequence E before the equity e
	fxBalanceVariant = `{"T":"balance.UNIFIED","P":15,"d":{"E":3,"e":null,"c":"USDT","b":"100","a":"60","h":"40","l":"5","tCM":"1","cPM":"2","cOM":"3","cUP":"4","iPM":"5","iOM":"6","iFF":"7","iUP":"8","cS":3,"U":1770116995059000000}}`
	// variant: the equity is present and the sequence E follows it
	fxBalanceVariant2 = `{"T":"balance.UNIFIED","P":16,"d":{"e":"11.5","E":4,"c":"ETH","b":"10","a":"9","h":"1","U":1770116995059000000}}`

	// 3470350 Position
	fxPosition = `{"P":1770117071746459555,"T":"positionAll.UNIFIED","d":{"O":1768876206493000000,"U":1770117071740000000,"l":"2","q":"-46","s":"TRUMPUSDTM","bP":"5.3580141244","eP":"4.8855208333333333333","iM":"9.7198","lP":"5.2913432001","mM":"CROSS","mP":"4.226","pV":"19.4396","pi":"70000000000000010","mmr":"0.012","mtM":"0.2332752","rPL":"-0.20267048333333333332","uPL":"3.03379583333333333332","r":"0.2001","adl":"0.46"}}`
	// variant: the single-symbol channel and distinct values
	fxPositionVariant = `{"T":"position.UNIFIED","P":17,"d":{"pi":"71","s":"XBTUSDTM","mM":"ISOLATED","q":"3","eP":"90000.5","pV":"270001.5","pM":"13500","mP":"90001","lP":"85000","bP":"84000","l":"20","uPL":"1.5","rPL":"-2.5","iM":"13500.1","mmr":"0.004","mtM":"1080","r":"0.65","adl":"0.12","U":9,"O":4}}`

	// 3470351 LiquidationWarning: the published example and the documented alternative push type
	fxLiquidationWarning = `{"T":"lw.UNIFIED","P":1729842192785164840,"d":{"eT":"MARGIN_CALL","r":"0.9378","a":"12456.32","iM":"58720.00","mM":"5284.80","aM":"1890.45","e":"63710.77","l":"51254.45","U":1729842192785100000}}`
	fxLiquidationRisk    = `{"T":"risk.UNIFIED","P":1729842252785164840,"d":{"eT":"FORCE_LIQUIDATION","r":"1.0123","a":"1","iM":"2","mM":"3","aM":"4","e":"5","l":"6","U":1729842252785100000}}`

	// 3470352 Leverage: the documented example is not valid JSON (commas are missing);
	// the payload is {s | c, l, mM, tT}
	fxLeverageFutures = `{"T":"leverage.UNIFIED","P":1764570290237940700,"d":{"s":"XBTUSDTM","l":"5.00","mM":"CROSS","tT":"FUTURES"}}`
	fxLeverageMargin  = `{"T":"leverage.UNIFIED","P":1764570290237940710,"d":{"c":"BTC","l":"5.00","mM":"CROSS","tT":"MARGIN"}}`
)

// payloadFixtures lists every official frame with a value of the type that decodes
// its payload; models_test.go checks them all.
var payloadFixtures = []struct {
	name   string
	frame  string
	target func() any
}{
	{"ticker spot", fxTickerSpot, func() any { return new(Ticker) }},
	{"ticker futures", fxTickerFutures, func() any { return new(Ticker) }},
	{"ticker variant", fxTickerVariant, func() any { return new(Ticker) }},
	{"kline spot", fxKlineSpot, func() any { return new(Kline) }},
	{"kline futures", fxKlineFutures, func() any { return new(Kline) }},
	{"kline variant", fxKlineVariant, func() any { return new(Kline) }},
	{"trade spot", fxTradeSpot, func() any { return new(Trade) }},
	{"trade futures", fxTradeFutures, func() any { return new(Trade) }},
	{"trade variant", fxTradeVariant, func() any { return new(Trade) }},
	{"obu spot bbo", fxObuSpotBBO, func() any { return new(OrderBookUpdate) }},
	{"obu spot increment", fxObuSpotIncrement, func() any { return new(OrderBookUpdate) }},
	{"obu spot 10ms", fxObuSpot10ms, func() any { return new(OrderBookUpdate) }},
	{"obu futures bbo", fxObuFuturesBBO, func() any { return new(OrderBookUpdate) }},
	{"obu futures increment", fxObuFuturesIncrement, func() any { return new(OrderBookUpdate) }},
	{"obu futures 10ms", fxObuFutures10ms, func() any { return new(OrderBookUpdate) }},
	{"obu futures rpi", fxObuFuturesRPI, func() any { return new(OrderBookUpdate) }},
	{"obu spot 5 live", fxObuSpot5Live, func() any { return new(OrderBookUpdate) }},
	{"obu futures 50 live", fxObuFutures50Live, func() any { return new(OrderBookUpdate) }},
	{"obu futures rpi live", fxObuFuturesRPILive, func() any { return new(OrderBookUpdate) }},
	{"mark price", fxMarkPrice, func() any { return new(MarkPrice) }},
	{"mark price live", fxMarkPriceLive, func() any { return new(MarkPrice) }},
	{"funding rate", fxFundingRate, func() any { return new(FundingRate) }},
	{"all funding rates", fxFundingAll, func() any { return new([]FundingRate) }},
	{"call auction", fxCallAuction, func() any { return new(callAuctionWire) }},
	{"call auction variant", fxCallAuctionVariant, func() any { return new(callAuctionWire) }},
	{"order spot live", fxOrderSpotLive, func() any { return new(OrderUpdate) }},
	{"order spot filled", fxOrderSpotFilled, func() any { return new(OrderUpdate) }},
	{"order conditional", fxOrderConditional, func() any { return new(OrderUpdate) }},
	{"order triggered", fxOrderTriggered, func() any { return new(OrderUpdate) }},
	{"order tpsl", fxOrderTPSL, func() any { return new(OrderUpdate) }},
	{"order canceled", fxOrderCanceled, func() any { return new(OrderUpdate) }},
	{"order unified", fxOrderUnified, func() any { return new(OrderUpdate) }},
	{"order partial variant", fxOrderPartialVariant, func() any { return new(OrderUpdate) }},
	{"order partial canceled variant", fxOrderPartialCanceled, func() any { return new(OrderUpdate) }},
	{"execution normal", fxExecutionNormal, func() any { return new(Execution) }},
	{"execution adl", fxExecutionADL, func() any { return new(Execution) }},
	{"execution liquid", fxExecutionLiquid, func() any { return new(Execution) }},
	{"execution settlement", fxExecutionSettlement, func() any { return new(Execution) }},
	{"execution variant", fxExecutionVariant, func() any { return new(Execution) }},
	{"execution lite spot", fxExecutionLiteSpot, func() any { return new(ExecutionLite) }},
	{"execution lite futures", fxExecutionLiteFutures, func() any { return new(ExecutionLite) }},
	{"execution lite margin", fxExecutionLiteMargin, func() any { return new(ExecutionLite) }},
	{"execution lite variant", fxExecutionLiteVariant, func() any { return new(ExecutionLite) }},
	{"balance unified", fxBalanceUnified, func() any { return new(BalanceUpdate) }},
	{"balance funding", fxBalanceFunding, func() any { return new(BalanceUpdate) }},
	{"balance variant", fxBalanceVariant, func() any { return new(BalanceUpdate) }},
	{"balance variant 2", fxBalanceVariant2, func() any { return new(BalanceUpdate) }},
	{"position", fxPosition, func() any { return new(PositionUpdate) }},
	{"position variant", fxPositionVariant, func() any { return new(PositionUpdate) }},
	{"liquidation warning", fxLiquidationWarning, func() any { return new(LiquidationWarning) }},
	{"liquidation risk", fxLiquidationRisk, func() any { return new(LiquidationWarning) }},
	{"leverage futures", fxLeverageFutures, func() any { return new(LeverageUpdate) }},
	{"leverage margin", fxLeverageMargin, func() any { return new(LeverageUpdate) }},
}
