package streaming

import "strings"

// Frames below are the worked examples of KuCoin's official Classic Spot
// WebSocket documentation (https://www.kucoin.com/docs-new/ page ids in the
// comments), with the documentation's inline comments removed and its
// syntax slips fixed so they are valid JSON. A few carry a "variant" or "live"
// suffix: "variant" has the same shape with values chosen so that a wrongly mapped
// field cannot go unnoticed, "live" is a frame captured from the public feed on
// 2026-10-03 that shows what the documentation does not.
const (
	// 3470063 Ticker (the example spells the time key "Time", the schema "time")
	spTicker = `{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"sequence":"1545896668986","price":"0.08","size":"0.011","bestAsk":"0.08","bestAskSize":"0.18","bestBid":"0.049","bestBidSize":"0.036","Time":1704873323416}}`
	// live: lower-case "time"
	spTickerLive = `{"topic":"/market/ticker:ETH-USDT","type":"message","subject":"trade.ticker","data":{"bestAsk":"2667.61","bestAskSize":"10.5900853","bestBid":"2667.6","bestBidSize":"0.8915","price":"2667.64","sequence":"23242050174","size":"0.002","time":1790984011673}}`

	// 3470064 All Tickers (the symbol is the subject)
	spAllTicker = `{"topic":"/market/ticker:all","type":"message","subject":"BTC-USDT","data":{"bestAsk":"67218.7","bestAskSize":"1.92318539","bestBid":"67218.6","bestBidSize":"0.01045638","price":"67220","sequence":"14691455768","size":"0.00004316","time":1729757723612}}`
	// live
	spAllTickerLive = `{"topic":"/market/ticker:all","type":"message","subject":"MOG-USDT","data":{"bestAsk":"0.0000001143","bestAskSize":"10171660581","bestBid":"0.000000114","bestBidSize":"110091671","price":"0.0000001141","sequence":"1226485690","size":"133465474","time":1790983975701}}`

	// 3470065 Symbol Snapshot (JSON numbers with long decimals: not one digit may be lost)
	spSymbolSnapshot = `{"topic":"/market/snapshot:BTC-USDT","type":"message","subject":"trade.snapshot","data":{"sequence":"14691517895","data":{"askSize":1.15955795,"averagePrice":66867.89967612,"baseCurrency":"BTC","bidSize":0.81772627,"board":1,"buy":67158.1,"changePrice":315.20000000000000000000,"changeRate":0.0047,"close":67158.1,"datetime":1729758286011,"high":67611.80000000000000000000,"lastTradedPrice":67158.1,"low":65257.10000000000000000000,"makerCoefficient":1.000000,"makerFeeRate":0.001,"marginTrade":true,"mark":0,"market":"USDS","marketChange1h":{"changePrice":-102.10000000000000000000,"changeRate":-0.0015,"high":67310.60000000000000000000,"low":67051.80000000000000000000,"open":67260.20000000000000000000,"vol":53.73698081000000000000,"volValue":3609965.13819127700000000000},"marketChange24h":{"changePrice":315.20000000000000000000,"changeRate":0.0047,"high":67611.80000000000000000000,"low":65257.10000000000000000000,"open":66842.90000000000000000000,"vol":2227.69895852000000000000,"volValue":147972941.07857507300000000000},"marketChange4h":{"changePrice":-166.30000000000000000000,"changeRate":-0.0024,"high":67476.60000000000000000000,"low":67051.80000000000000000000,"open":67324.40000000000000000000,"vol":173.76971188000000000000,"volValue":11695949.43841656500000000000},"markets":["USDS","PoW"],"open":66842.90000000000000000000,"quoteCurrency":"USDT","sell":67158.2,"siteTypes":["turkey","thailand","global"],"sort":100,"symbol":"BTC-USDT","symbolCode":"BTC-USDT","takerCoefficient":1.000000,"takerFeeRate":0.001,"trading":true,"vol":2227.69895852000000000000,"volValue":147972941.07857507300000000000}}}`
	// live: the snapshot also carries lastSize and its own sequence
	spSymbolSnapshotLive = `{"topic":"/market/snapshot:BTC-USDT","type":"message","subject":"trade.snapshot","data":{"sequence":"38110100812","data":{"askSize":0.25443497,"averagePrice":85713.700425,"baseCurrency":"BTC","bidSize":0.34401361,"board":1,"buy":84520.9,"changePrice":-255.3,"changeRate":-0.0030,"close":84521,"datetime":1790984037207,"high":87220,"lastSize":0.00149977,"lastTradedPrice":84521,"low":83892.5,"makerCoefficient":1.0000,"makerFeeRate":0.001,"marginTrade":true,"mark":0,"market":"USDS","marketChange1h":{"changePrice":-8.7,"changeRate":-0.0001,"high":84555.7,"low":84483.1,"open":84529.7,"vol":30.4787595734952392,"volValue":2576256.90421548792765776},"marketChange24h":{"changePrice":-255.3,"changeRate":-0.0030,"high":87220,"low":83892.5,"open":84776.3,"vol":4035.7326718967590488,"volValue":345900144.63267467635328976},"marketChange4h":{"changePrice":388.6,"changeRate":0.0046,"high":84555.7,"low":84099.4,"open":84132.4,"vol":300.1616207134952392,"volValue":25332317.25051070792765776},"markets":["USDS","Majors","PoW","Layer1"],"open":84776.3,"quoteCurrency":"USDT","sell":84521,"sequence":"38110100812","siteTypes":["global"],"sort":100,"symbol":"BTC-USDT","symbolCode":"BTC-USDT","takerCoefficient":1.0000,"takerFeeRate":0.001,"trading":true,"vol":4035.7326718967590488,"volValue":345900144.63267467635328976}}}`

	// 3470066 Market Snapshot (topic names the market, the payload the symbol; outer sequence is a number)
	spMarketSnapshot = `{"topic":"/market/snapshot:BTC","type":"message","subject":"trade.snapshot","data":{"sequence":1729785948015,"data":{"askSize":1375.1096,"averagePrice":0.00000262,"baseCurrency":"CHR","bidSize":152.0912,"board":0,"buy":0.00000263,"changePrice":0.00000005300000000000,"changeRate":0.0200,"close":0.000002698,"datetime":1729785948008,"high":0.00000274600000000000,"lastTradedPrice":0.000002698,"low":0.00000255800000000000,"makerCoefficient":1.000000,"makerFeeRate":0.001,"marginTrade":false,"mark":0,"market":"BTC","marketChange1h":{"changePrice":-0.00000000900000000000,"changeRate":-0.0033,"high":0.00000270700000000000,"low":0.00000264200000000000,"open":0.00000270700000000000,"vol":27.10350000000000000000,"volValue":0.00007185015660000000},"marketChange24h":{"changePrice":0.00000005300000000000,"changeRate":0.0200,"high":0.00000274600000000000,"low":0.00000255800000000000,"open":0.00000264500000000000,"vol":6824.94800000000000000000,"volValue":0.01789509649520000000},"marketChange4h":{"changePrice":0.00000000600000000000,"changeRate":0.0022,"high":0.00000270700000000000,"low":0.00000264200000000000,"open":0.00000269200000000000,"vol":92.69020000000000000000,"volValue":0.00024903875740000000},"markets":["BTC","DePIN","Layer 1"],"open":0.00000264500000000000,"quoteCurrency":"BTC","sell":0.000002695,"siteTypes":["global"],"sort":100,"symbol":"CHR-BTC","symbolCode":"CHR-BTC","takerCoefficient":1.000000,"takerFeeRate":0.001,"trading":true,"vol":6824.94800000000000000000,"volValue":0.01789509649520000000}}}`
	// live: the symbol's own sequence inside the snapshot differs from the outer clock-like sequence
	spMarketSnapshotLive = `{"topic":"/market/snapshot:BTC","type":"message","subject":"trade.snapshot","data":{"sequence":1790984045980,"data":{"askSize":107.83,"averagePrice":0.00000461,"baseCurrency":"EWT","bidSize":6,"board":0,"buy":0.000004479,"changePrice":-0.000000036,"changeRate":-0.0079,"close":0.000004484,"datetime":1790984045973,"high":0.000005194,"lastSize":16.1,"lastTradedPrice":0.000004484,"low":0.000004389,"makerCoefficient":3.0000,"makerFeeRate":0.001,"marginTrade":false,"mark":0,"market":"BTC","marketChange1h":{"changePrice":-0.000000075,"changeRate":-0.0164,"high":0.000004602,"low":0.000004484,"open":0.000004559,"vol":1375.04,"volValue":0.00624406432},"marketChange24h":{"changePrice":-0.000000036,"changeRate":-0.0079,"high":0.000005194,"low":0.000004389,"open":0.00000452,"vol":99842.96,"volValue":0.45926894328},"marketChange4h":{"changePrice":0.000000039,"changeRate":0.0087,"high":0.000004902,"low":0.00000439,"open":0.000004445,"vol":22519.1,"volValue":0.10162316112},"markets":["BTC"],"open":0.00000452,"quoteCurrency":"BTC","sell":0.000004533,"sequence":"352089952","siteTypes":["global"],"sort":100,"symbol":"EWT-BTC","symbolCode":"EWT-BTC","takerCoefficient":3.0000,"takerFeeRate":0.001,"trading":true,"vol":99842.96,"volValue":0.45926894328}}}`

	// 3470067 Level 1 (a flat [price, size] pair per side)
	spLevel1 = `{"topic":"/spotMarket/level1:BTC-USDT","type":"message","subject":"level1","data":{"asks":["68145.8","0.51987471"],"bids":["68145.7","1.29267802"],"timestamp":1729816058766}}`
	// variant: a market with no sell orders at all
	spLevel1NoAsks = `{"topic":"/spotMarket/level1:ETH-USDT","type":"message","subject":"level1","data":{"asks":[],"bids":["2000.5","3"],"timestamp":1729816058767}}`

	// 3470068 Orderbook Increment
	spLevel2 = `{"topic":"/market/level2:BTC-USDT","type":"message","subject":"trade.l2update","data":{"changes":{"asks":[["67993.3","1.21427407","14701689783"]],"bids":[]},"sequenceEnd":14701689783,"sequenceStart":14701689783,"symbol":"BTC-USDT","time":1729816425625}}`
	// variant of a live push: a range of sequence numbers, both sides in one push, a removal
	spLevel2Range = `{"topic":"/market/level2:BTC-USDT","type":"message","subject":"trade.l2update","data":{"changes":{"asks":[["84271.2","0","38110279538"]],"bids":[["1","13329.23415421","38110279535"],["82003.8","0.00073166","38110279536"]]},"sequenceEnd":38110279538,"sequenceStart":38110279535,"symbol":"BTC-USDT","time":1790984501513}}`

	// 3470069 Level 5
	spDepth5 = `{"topic":"/spotMarket/level2Depth5:BTC-USDT","type":"message","subject":"level2","data":{"asks":[["67996.7","1.14213262"],["67996.8","0.21748212"]],"bids":[["67996.6","0.37969491"],["67995.3","0.20779746"]],"timestamp":1729822226746}}`

	// 3470070 Level 50
	spDepth50 = `{"topic":"/spotMarket/level2Depth50:BTC-USDT","type":"message","subject":"level2","data":{"asks":[["95964.3","0.08168874"],["95967.9","0.00985094"]],"bids":[["95964.2","1.35483359"],["95964.1","0.01117492"]],"timestamp":1733124805073}}`

	// 3470071 Klines (open, close, high and low are all different)
	spKline = `{"topic":"/market/candles:BTC-USDT_1hour","type":"message","subject":"trade.candles.update","data":{"symbol":"BTC-USDT","candles":["1729839600","67644.9","67437.6","67724.8","67243.8","44.88321441","3027558.991928447"],"time":1729842192785164840}}`

	// 3470072 Trade (the time is a numeric string of nanoseconds)
	spTrade = `{"topic":"/market/match:BTC-USDT","type":"message","subject":"trade.l3match","data":{"makerOrderId":"671b5007389355000701b1d3","price":"67523","sequence":"11067996711960577","side":"buy","size":"0.003","symbol":"BTC-USDT","takerOrderId":"671b50161777ff00074c168d","time":"1729843222921000000","tradeId":"11067996711960577","type":"match"}}`

	// 3470137 Call Auction Orderbook - Level 50
	spCallAuctionDepth50 = `{"topic":"/callauction/level2Depth50:BTC-USDT","type":"message","subject":"level2","data":{"asks":[["95964.3","0.08168874"],["95967.9","0.00985094"]],"bids":[["95964.2","1.35483359"],["95964.1","0.01117492"]],"timestamp":1733124805073}}`

	// 3470138 Call Auction Data (the example's full names; the schema lists abbreviations)
	spCallAuctionData = `{"type":"message","topic":"/callauction/callauctionData:BTC-USDT","subject":"callauction.callauctionData","data":{"symbol":"BTC-USDT","estimatedPrice":"0.17","estimatedSize":"0.03715004","sellOrderRangeLowPrice":"1.788","sellOrderRangeHighPrice":"2.788","buyOrderRangeLowPrice":"1.788","buyOrderRangeHighPrice":"2.788","time":1550653727731}}`
	// variant: the abbreviated spelling of the documentation's schema, with values that differ per field
	spCallAuctionDataAbbreviated = `{"type":"message","topic":"/callauction/callauctionData:ETH-USDT","subject":"callauction.callauctionData","data":{"s":"ETH-USDT","ep":"2001.5","es":"3.5","slp":"1990","shp":"2010","blp":"1980","bhp":"2020","ts":1550653727732}}`

	// 3470073 Order V2: received, open, update, match, filled, canceled
	spOrderReceived = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"clientOid":"5c52e11203aa677f33e493fc","orderId":"6720da3fa30a360007f5f832","orderTime":1730206271588,"orderType":"market","originSize":"0.00001","side":"buy","status":"new","symbol":"BTC-USDT","ts":1730206271616000000,"type":"received"}}`
	spOrderOpen     = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0","clientOid":"5c52e11203aa677f33e493fb","filledSize":"0","orderId":"6720ecd9ec71f4000747731a","orderTime":1730211033305,"orderType":"limit","originSize":"0.00001","price":"50000","remainSize":"0.00001","side":"buy","size":"0.00001","status":"open","symbol":"BTC-USDT","ts":1730211033335000000,"type":"open"}}`
	spOrderUpdate   = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0.00001","clientOid":"5c52e11203aa677f33e493fb","filledSize":"0","oldSize":"0.00002","orderId":"6720df7640e6fe0007b57696","orderTime":1730207606848,"orderType":"limit","originSize":"0.00002","price":"50000","remainSize":"0.00001","side":"buy","size":"0.00001","status":"open","symbol":"BTC-USDT","ts":1730207616617000000,"type":"update"}}`
	spOrderMatch    = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0","clientOid":"5c52e11203aa677f33e493fc","feeType":"takerFee","filledSize":"0.00001","liquidity":"taker","matchPrice":"71171.9","matchSize":"0.00001","orderId":"6720da3fa30a360007f5f832","orderTime":1730206271588,"orderType":"market","originSize":"0.00001","remainSize":"0","side":"buy","size":"0.00001","status":"match","symbol":"BTC-USDT","tradeId":"11116472408358913","ts":1730206271616000000,"type":"match"}}`
	spOrderFilled   = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0","clientOid":"5c52e11203aa677f33e493fc","filledSize":"0.00001","orderId":"6720da3fa30a360007f5f832","orderTime":1730206271588,"orderType":"market","originSize":"0.00001","remainFunds":"0","remainSize":"0","side":"buy","size":"0.00001","status":"done","symbol":"BTC-USDT","ts":1730206271616000000,"type":"filled"}}`
	spOrderCanceled = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0.00002","clientOid":"5c52e11203aa677f33e493fb","filledSize":"0","orderId":"6720df7640e6fe0007b57696","orderTime":1730207606848,"orderType":"limit","originSize":"0.00002","price":"50000","remainFunds":"0","remainSize":"0","side":"buy","size":"0.00001","status":"done","symbol":"BTC-USDT","ts":1730207624559000000,"type":"canceled"}}`

	// 3470075 Balance (the time is a numeric string of milliseconds)
	spBalance = `{"topic":"/account/balance","type":"message","subject":"account.balance","id":"354689988084000","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"accountId":"548674591753","currency":"USDT","total":"21.133773386762","available":"20.132773386762","hold":"1.001","availableChange":"-0.5005","holdChange":"0.5005","relationContext":{"symbol":"BTC-USDT","orderId":"6721d0632db25b0007071fdc","tradeId":"11116472408358913"},"relationEvent":"trade.hold","relationEventId":"354689988084000","time":"1730269283892"}}`
	// variant: an isolated-margin settlement without a trade context and a numeric time
	spBalanceIsolated = `{"topic":"/account/balance","type":"message","subject":"account.balance","id":"354689988084001","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"accountId":"548674591754","currency":"BTC","total":"0.5","available":"0.25","hold":"0.25","availableChange":"0.1","holdChange":"-0.1","relationContext":{},"relationEvent":"isolated_BTC-USDT.setted","relationEventId":"354689988084001","time":1730269283893}}`

	// 3470139 Stop Order
	spStopOrder = `{"topic":"/spotMarket/advancedOrders","type":"message","subject":"stopOrder","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"orderId":"vs93gpupfa48anof003u85mb","orderPrice":"70000","orderType":"stop","side":"buy","size":"0.00007142","stop":"loss","stopPrice":"71000","symbol":"BTC-USDT","tradeType":"TRADE","type":"open","createdAt":1742305928064,"ts":1742305928091268493}}`
	// variant: a margin stop order that was triggered (the documentation spells the event in capitals) and one that was cancelled
	spStopOrderTriggered = `{"topic":"/spotMarket/advancedOrders","type":"message","subject":"stopOrder","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"orderId":"vs93gpupfa48anof003u85mc","orderPrice":"70000","orderType":"stop","side":"sell","size":"1.5","stop":"entry","stopPrice":"69000","symbol":"ETH-USDT","tradeType":"MARGIN_TRADE","type":"TRIGGERED","createdAt":1742305928065,"ts":1742305928091268494}}`
	spStopOrderCanceled  = `{"topic":"/spotMarket/advancedOrders","type":"message","subject":"stopOrder","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"orderId":"vs93gpupfa48anof003u85md","orderPrice":"70000","orderType":"stop","side":"buy","size":"2","stop":"loss","stopPrice":"71000","symbol":"BTC-USDT","tradeType":"MARGIN_ISOLATED_TRADE","type":"cancel","createdAt":1742305928066,"ts":1742305928091268495}}`
)

// The Order V1 page repeats the V2 examples (without the "received" event) on its
// own topic.
var (
	spOrderV1Open     = strings.ReplaceAll(spOrderOpen, "tradeOrdersV2", "tradeOrders")
	spOrderV1Update   = strings.ReplaceAll(spOrderUpdate, "tradeOrdersV2", "tradeOrders")
	spOrderV1Match    = strings.ReplaceAll(spOrderMatch, "tradeOrdersV2", "tradeOrders")
	spOrderV1Filled   = strings.ReplaceAll(spOrderFilled, "tradeOrdersV2", "tradeOrders")
	spOrderV1Canceled = strings.ReplaceAll(spOrderCanceled, "tradeOrdersV2", "tradeOrders")
)

// allFixtures lists every frame above for the JSON validity check.
var allFixtures = map[string]string{
	"spTicker": spTicker, "spTickerLive": spTickerLive, "spAllTicker": spAllTicker, "spAllTickerLive": spAllTickerLive,
	"spSymbolSnapshot": spSymbolSnapshot, "spSymbolSnapshotLive": spSymbolSnapshotLive,
	"spMarketSnapshot": spMarketSnapshot, "spMarketSnapshotLive": spMarketSnapshotLive,
	"spLevel1": spLevel1, "spLevel1NoAsks": spLevel1NoAsks, "spLevel2": spLevel2, "spLevel2Range": spLevel2Range,
	"spDepth5": spDepth5, "spDepth50": spDepth50, "spKline": spKline, "spTrade": spTrade,
	"spCallAuctionDepth50": spCallAuctionDepth50, "spCallAuctionData": spCallAuctionData, "spCallAuctionDataAbbreviated": spCallAuctionDataAbbreviated,
	"spOrderReceived": spOrderReceived, "spOrderOpen": spOrderOpen, "spOrderUpdate": spOrderUpdate, "spOrderMatch": spOrderMatch,
	"spOrderFilled": spOrderFilled, "spOrderCanceled": spOrderCanceled,
	"spOrderV1Open": spOrderV1Open, "spOrderV1Update": spOrderV1Update, "spOrderV1Match": spOrderV1Match,
	"spOrderV1Filled": spOrderV1Filled, "spOrderV1Canceled": spOrderV1Canceled,
	"spBalance": spBalance, "spBalanceIsolated": spBalanceIsolated,
	"spStopOrder": spStopOrder, "spStopOrderTriggered": spStopOrderTriggered, "spStopOrderCanceled": spStopOrderCanceled,
}
