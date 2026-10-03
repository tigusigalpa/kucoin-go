package streaming

// Frames below are the worked examples of KuCoin's official Classic Margin
// WebSocket documentation (https://www.kucoin.com/docs-new/ page ids in the
// comments), with the documentation's inline comments removed and its syntax slips
// fixed so they are valid JSON. A "variant" suffix marks the same shape with values
// chosen so that a wrongly mapped field cannot go unnoticed, "live" a frame
// captured from the public feed on 2026-10-03.
const (
	// 3470076 Index Price (the example carries an id, live pushes do not)
	mgIndex = `{"id":"5c24c5da03aa673885cd67a0","type":"message","topic":"/indicator/index:USDT-BTC","subject":"tick","data":{"symbol":"USDT-BTC","granularity":5000,"timestamp":1551770400000,"value":0.0001092}}`
	// live: no id, a JSON number with 15 decimals
	mgIndexLive = `{"topic":"/indicator/index:ETH-BTC","type":"message","subject":"tick","data":{"symbol":"ETH-BTC","granularity":1000,"value":0.031560000000000,"timestamp":1790984308000}}`

	// 3470077 Mark Price
	mgMark = `{"topic":"/indicator/markPrice:USDT-BTC","type":"message","subject":"tick","data":{"symbol":"USDT-BTC","granularity":1000,"value":0.000011820000000,"timestamp":1740840036000}}`
	// live
	mgMarkLive = `{"topic":"/indicator/markPrice:ETH-BTC","type":"message","subject":"tick","data":{"symbol":"ETH-BTC","granularity":1000,"value":0.031560000000000,"timestamp":1790984308000}}`

	// 3470078 Cross Margin Position, subject debt.ratio (the example misses the comma after "private")
	mgCrossDebtRatio = `{"topic":"/margin/position","subject":"debt.ratio","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"debtRatio":0,"totalAsset":0.00052431772284080000000,"marginCoefficientTotalAsset":"0.0005243177228408","totalDebt":"0","assetList":{"BTC":{"total":"0.00002","available":"0","hold":"0.00002"},"USDT":{"total":"33.68855864","available":"15.01916691","hold":"18.66939173"}},"debtList":{"BTC":"0","USDT":"0"},"timestamp":1729912435657}}`
	// variant: a liability, with values that differ per field and currency
	mgCrossDebtRatioVariant = `{"topic":"/margin/position","subject":"debt.ratio","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"debtRatio":"0.42","totalAsset":"1.5","marginCoefficientTotalAsset":1.25,"totalDebt":"0.63","assetList":{"ETH":{"total":"10","available":"4","hold":"6"}},"debtList":{"ETH":"3.5","USDT":"120.25"},"timestamp":1729912438657}}`
	// 3470078 Cross Margin Position, subject position.status (the documentation shows this
	// payload under the subject "debt.ratio" although it labels it position.status; its
	// timestamp, 15538460812100, is copied here as printed)
	mgCrossStatus = `{"topic":"/margin/position","subject":"position.status","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"type":"FROZEN_FL","timestamp":15538460812100}}`
	// the same frame exactly as the documentation prints it, with the subject "debt.ratio"
	mgCrossStatusAsPrinted = `{"topic":"/margin/position","subject":"debt.ratio","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"type":"FROZEN_FL","timestamp":15538460812100}}`

	// 3470079 Isolated Margin Position (the example misses the comma after "private")
	mgIsolated = `{"topic":"/margin/isolatedPosition:BTC-USDT","subject":"positionChange","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"tag":"BTC-USDT","status":"DEBT","statusBizType":"DEFAULT_DEBT","accumulatedPrincipal":"5.01","changeAssets":{"BTC":{"total":"0.00043478","hold":"0","liabilityPrincipal":"0","liabilityInterest":"0"},"USDT":{"total":"0.98092004","hold":"0","liabilityPrincipal":"26","liabilityInterest":"0.00025644"}},"timestamp":1730121097742}}`
	// variant: another symbol and status
	mgIsolatedVariant = `{"topic":"/margin/isolatedPosition:ETH-USDT","subject":"positionChange","type":"message","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"tag":"ETH-USDT","status":"IN_LIQUIDATION","statusBizType":"FORCE_LIQUIDATION","accumulatedPrincipal":12.5,"changeAssets":{"ETH":{"total":"1","hold":"0.5","liabilityPrincipal":"0.25","liabilityInterest":"0.001"}},"timestamp":1730121097743}}`

	// 3470256 Order V2 and 3470257 Order V1 reuse the Spot specification (3470073, 3470074):
	// the frames are the Spot examples
	mgOrderV2Open  = `{"topic":"/spotMarket/tradeOrdersV2","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0","clientOid":"5c52e11203aa677f33e493fb","filledSize":"0","orderId":"6720ecd9ec71f4000747731a","orderTime":1730211033305,"orderType":"limit","originSize":"0.00001","price":"50000","remainSize":"0.00001","side":"buy","size":"0.00001","status":"open","symbol":"BTC-USDT","ts":1730211033335000000,"type":"open"}}`
	mgOrderV1Match = `{"topic":"/spotMarket/tradeOrders","type":"message","subject":"orderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"canceledSize":"0","clientOid":"5c52e11203aa677f33e493fc","feeType":"takerFee","filledSize":"0.00001","liquidity":"taker","matchPrice":"71171.9","matchSize":"0.00001","orderId":"6720da3fa30a360007f5f832","orderTime":1730206271588,"orderType":"market","originSize":"0.00001","remainSize":"0","side":"buy","size":"0.00001","status":"match","symbol":"BTC-USDT","tradeId":"11116472408358913","ts":1730206271616000000,"type":"match"}}`

	// 3470258 Balance reuses the Spot specification (3470075); the margin account kinds name
	// themselves in relationEvent
	mgBalanceMargin   = `{"topic":"/account/balance","type":"message","subject":"account.balance","id":"354689988084002","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"accountId":"548674591755","currency":"USDT","total":"21.133773386762","available":"20.132773386762","hold":"1.001","availableChange":"-0.5005","holdChange":"0.5005","relationContext":{"symbol":"BTC-USDT","orderId":"6721d0632db25b0007071fdc","tradeId":"11116472408358913"},"relationEvent":"margin.hold","relationEventId":"354689988084002","time":"1730269283892"}}`
	mgBalanceIsolated = `{"topic":"/account/balance","type":"message","subject":"account.balance","id":"354689988084003","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"accountId":"548674591756","currency":"BTC","total":"0.5","available":"0.25","hold":"0.25","availableChange":"0.1","holdChange":"-0.1","relationContext":{"symbol":"BTC-USDT"},"relationEvent":"isolatedV2_BTC-USDT.transfer","relationEventId":"354689988084003","time":1730269283893}}`

	// 3470259 Stop Order reuses the Spot specification (3470139); margin stop orders carry
	// their account kind in tradeType
	mgStopOrderMargin   = `{"topic":"/spotMarket/advancedOrders","type":"message","subject":"stopOrder","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"orderId":"vs93gpupfa48anof003u85mc","orderPrice":"70000","orderType":"stop","side":"sell","size":"1.5","stop":"entry","stopPrice":"69000","symbol":"ETH-USDT","tradeType":"MARGIN_TRADE","type":"TRIGGERED","createdAt":1742305928065,"ts":1742305928091268494}}`
	mgStopOrderIsolated = `{"topic":"/spotMarket/advancedOrders","type":"message","subject":"stopOrder","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"orderId":"vs93gpupfa48anof003u85md","orderPrice":"70000","orderType":"stop","side":"buy","size":"2","stop":"loss","stopPrice":"71000","symbol":"BTC-USDT","tradeType":"MARGIN_ISOLATED_TRADE","type":"open","createdAt":1742305928066,"ts":1742305928091268495}}`

	// a Spot public frame (3470063), for the Spot channels that a Margin session serves too
	mgSpotTicker = `{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"sequence":"1545896668986","price":"0.08","size":"0.011","bestAsk":"0.08","bestAskSize":"0.18","bestBid":"0.049","bestBidSize":"0.036","Time":1704873323416}}`
)

// allFixtures lists every frame above for the JSON validity check.
var allFixtures = map[string]string{
	"mgIndex": mgIndex, "mgIndexLive": mgIndexLive, "mgMark": mgMark, "mgMarkLive": mgMarkLive,
	"mgCrossDebtRatio": mgCrossDebtRatio, "mgCrossDebtRatioVariant": mgCrossDebtRatioVariant,
	"mgCrossStatus": mgCrossStatus, "mgCrossStatusAsPrinted": mgCrossStatusAsPrinted,
	"mgIsolated": mgIsolated, "mgIsolatedVariant": mgIsolatedVariant,
	"mgOrderV2Open": mgOrderV2Open, "mgOrderV1Match": mgOrderV1Match,
	"mgBalanceMargin": mgBalanceMargin, "mgBalanceIsolated": mgBalanceIsolated,
	"mgStopOrderMargin": mgStopOrderMargin, "mgStopOrderIsolated": mgStopOrderIsolated,
	"mgSpotTicker": mgSpotTicker,
}
