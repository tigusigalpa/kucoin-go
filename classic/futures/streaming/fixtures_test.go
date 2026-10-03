package streaming

// Frames below are the worked examples of KuCoin's official Classic Futures
// WebSocket documentation (https://www.kucoin.com/docs-new/ page ids in the
// comments), with the documentation's inline comments removed so they are valid
// JSON. A few carry a "variant" suffix: the same shape with values chosen so that
// a wrongly mapped field cannot go unnoticed.
const (
	// 3470080 Ticker V2
	fxTickerV2 = `{"topic":"/contractMarket/tickerV2:XBTUSDTM","type":"message","subject":"tickerV2","sn":1713516609293,"data":{"symbol":"XBTUSDTM","sequence":1713516609293,"bestBidSize":5044,"bestBidPrice":"86454.5","bestAskPrice":"86454.6","bestAskSize":73,"ts":1740641976241000000}}`

	// 3470081 Ticker V1
	fxTickerV1 = `{"topic":"/contractMarket/ticker:XBTUSDTM","type":"message","subject":"ticker","sn":1828964168748,"data":{"symbol":"XBTUSDTM","sequence":1828964168748,"side":"buy","size":2,"price":"86429.7","bestBidSize":112,"bestBidPrice":"86429.6","bestAskPrice":"86429.7","tradeId":"1828964168748","bestAskSize":1578,"ts":1740642161735000000}}`

	// 3470083 Orderbook - Level 5
	fxDepth5 = `{"topic":"/contractMarket/level2Depth5:XBTUSDTM","type":"message","subject":"level2","sn":1731680019100,"data":{"bids":[["89720.9",513],["89720.8",12],["89718.6",113],["89718.4",19],["89718.3",7]],"sequence":1709294294670,"timestamp":1731680019100,"ts":1731680019100,"asks":[["89721",906],["89721.1",203],["89721.4",113],["89723.2",113],["89725.4",113]]}}`

	// 3470097 Orderbook - Level 50
	fxDepth50 = `{"topic":"/contractMarket/level2Depth50:XBTUSDTM","type":"message","subject":"level2","sn":1731680249700,"data":{"bids":[["89778.6",1534],["89778.2",54]],"sequence":1709294490099,"timestamp":1731680249700,"ts":1731680249700,"asks":[["89778.7",854],["89779.2",4]]}}`

	// 3470082 Orderbook - Increment
	fxLevel2 = `{"topic":"/contractMarket/level2:XBTUSDTM","type":"message","subject":"level2","sn":1709400450243,"data":{"sequence":1709400450243,"change":"90631.2,sell,2","timestamp":1731897467182}}`

	// 3470086 Klines, as captured from the live feed on 2026-10-03: the candle is
	// [start, open, close, high, low, turnover (quote currency), volume (contracts)];
	// the same minute's REST kline reports volume 12785 and turnover 1080991.7836.
	fxKline = `{"topic":"/contractMarket/limitCandle:XBTUSDTM_1min","type":"message","data":{"symbol":"XBTUSDTM","candles":["1790993280","84568.3","84556.7","84568.3","84544.2","1080991.7836","12785"],"time":1790993360463},"subject":"candle.stick"}`
	// variant: distinct open/close/high/low/turnover/volume to prove the whole mapping
	fxKlineVariant = `{"topic":"/contractMarket/limitCandle:ETHUSDTM_5min","type":"message","data":{"symbol":"ETHUSDTM","candles":["1731898200","2.1","2.4","2.9","2.0","71.5","30"],"time":1731898208357},"subject":"candle.stick"}`

	// 3470084 Trade
	fxTrade = `{"topic":"/contractMarket/execution:XBTUSDTM","type":"message","subject":"match","sn":1794100537695,"data":{"symbol":"XBTUSDTM","sequence":1794100537695,"side":"buy","size":2,"price":"90503.9","takerOrderId":"247822202957807616","makerOrderId":"247822167163555840","tradeId":"1794100537695","ts":1731898619520000000}}`

	// 3470087 Instrument: mark/index price and funding rate (JSON numbers, as documented)
	fxInstrumentMark    = `{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"mark.index.price","data":{"markPrice":90445.02,"indexPrice":90445.02,"granularity":1000,"timestamp":1731899129000}}`
	fxInstrumentFunding = `{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"funding.rate","data":{"granularity":60000,"fundingRate":-0.002966,"timestamp":1551770400000}}`
	// the live feed adds "period" to the funding-rate push (captured 2026-10-03)
	fxInstrumentFundingLive = `{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"funding.rate","data":{"period":1,"granularity":60000,"fundingRate":-0.000002,"timestamp":1790993340000}}`

	// 3470088 Funding Fee Settlement
	fxFundingBegin = `{"type":"message","topic":"/contract/announcement","subject":"funding.begin","data":{"symbol":"XBTUSDTM","fundingTime":1551770400000,"fundingRate":-0.002966,"timestamp":1551770400000}}`
	fxFundingEnd   = `{"type":"message","topic":"/contract/announcement","subject":"funding.end","data":{"symbol":"XBTUSDTM","fundingTime":1551770400000,"fundingRate":-0.002966,"timestamp":1551770410000}}`

	// 3470089 Symbol Snapshot (turnover has 19 significant digits: it must not be rounded)
	fxSnapshot = `{"topic":"/contractMarket/snapshot:XBTUSDTM","type":"message","subject":"snapshot.24h","id":"67c01b710329940001118cb1","data":{"highPrice":89299.9,"lastPrice":86262.6,"lowPrice":82205.2,"price24HoursBefore":88762.5,"priceChg":-2499.9,"priceChgPct":-0.0281,"symbol":"XBTUSDTM","turnover":1033552780.2532196044,"volume":12062.039,"ts":1740643185017646670}}`

	// the live feed adds "fundingRate" to every snapshot (captured 2026-10-03)
	fxSnapshotLive = `{"topic":"/contractMarket/snapshot:XBTUSDTM","type":"message","subject":"snapshot.24h","data":{"fundingRate":-0.000002,"highPrice":87274.8,"lastPrice":84556.7,"lowPrice":83864.7,"price24HoursBefore":85034.0,"priceChg":-477.3,"priceChgPct":-0.0056,"symbol":"XBTUSDTM","ts":1790993335003748866,"turnover":337376435.2988,"volume":3938.189}}`

	// 3470090 Orders
	fxOrderOpen     = `{"topic":"/contractMarket/tradeOrders:XBTUSDTM","type":"message","subject":"symbolOrderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"symbol":"XBTUSDTM","tradeType":"trade","side":"buy","canceledSize":"0","orderId":"247899236673269761","liquidity":"maker","marginMode":"ISOLATED","type":"open","orderTime":1731916985768138917,"size":"1","filledSize":"0","price":"91670","remainSize":"1","status":"open","ts":1731916985789000000}}`
	fxOrderUpdate   = `{"topic":"/contractMarket/tradeOrders","type":"message","subject":"orderChange","userId":"669a61642857ca000186f626","channelType":"private","data":{"symbol":"RUNEUSDTM","orderType":"limit","tradeType":"trade","side":"buy","canceledSize":"1037","orderId":"228685469427204099","liquidity":"maker","marginMode":"ISOLATED","type":"update","userId":"669a61642857ca000186f626","oldSize":"19982","orderTime":1727336066682194084,"size":"19982","filledSize":"0","price":"5.029","remainSize":"11618","clientOid":"10496pp066R679264","status":"open","ts":1727336066766000000}}`
	fxOrderMatch    = `{"topic":"/contractMarket/tradeOrders:XBTUSDTM","type":"message","subject":"symbolOrderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"symbol":"XBTUSDTM","orderType":"limit","tradeType":"trade","side":"buy","canceledSize":"0","orderId":"247899236673269761","liquidity":"maker","marginMode":"ISOLATED","type":"match","feeType":"makerFee","orderTime":1731916985768138917,"size":"1","filledSize":"1","price":"91670","matchPrice":"91670","matchSize":"1","remainSize":"0","tradeId":"1794175373644","status":"done","ts":1731916996762000000}}`
	fxOrderLiquid   = `{"topic":"/contractMarket/tradeOrders:XBTUSDTM","type":"message","subject":"symbolOrderChange","userId":"6356450001cef524","channelType":"private","data":{"symbol":"XBTUSDTM","orderType":"limit","side":"sell","canceledSize":"0","orderId":"440761625608192","liquidity":"taker","marginMode":"ISOLATED","type":"match","feeType":"takerFee","orderTime":1743146786640000000,"size":"3840","filledSize":"1116","price":"84603.44","matchPrice":"85739.69","matchSize":"1000","remainSize":"2724","tradeId":"1740800012709","tradeType":"liquid","status":"match","ts":1743146786746000000}}`
	fxOrderADL      = `{"topic":"/contractMarket/tradeOrders","type":"message","subject":"orderChange","userId":"665d1df19c51ab0001029a49","channelType":"private","data":{"symbol":"10PEPEUSDTM","orderType":"limit","side":"sell","canceledSize":"0","orderId":"1961728417792","positionSide":"BOTH","liquidity":"taker","marginMode":"ISOLATED","type":"match","feeType":"takerFee","orderTime":1750839892050000000,"size":"100","filledSize":"100","price":"0.0000126","matchPrice":"0.0000126","matchSize":"100","remainSize":"0","tradeId":"1750839397535","tradeType":"adl","status":"match","ts":1750839892050000000}}`
	fxOrderFilled   = `{"topic":"/contractMarket/tradeOrders:XBTUSDTM","type":"message","subject":"symbolOrderChange","userId":"6988447c2814ec00015d5726","channelType":"private","data":{"symbol":"XBTUSDTM","orderType":"limit","side":"buy","canceledSize":"0","orderId":"435278894182961153","positionSide":"BOTH","marginMode":"CROSS","type":"filled","orderTime":1776591777464496313,"size":"1","filledSize":"1","price":"76000","allCanceledSize":"0","remainSize":"0","clientOid":"5c52e11203aa677f33e493fb","tradeType":"trade","status":"done","ts":1776591777474000000}}`
	fxOrderCanceled = `{"topic":"/contractMarket/tradeOrders:XBTUSDTM","type":"message","subject":"symbolOrderChange","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"symbol":"XBTUSDTM","orderType":"limit","tradeType":"trade","side":"buy","canceledSize":"1","orderId":"247901211536203776","marginMode":"ISOLATED","type":"canceled","orderTime":1731917456611809239,"size":"1","filledSize":"0","price":"90000","remainSize":"0","status":"done","ts":1731917460806000000}}`

	// 3470091 Stop Orders (the documentation shows it as a JS object literal; here as JSON; size is a number)
	fxStopOrder = `{"topic":"/contractMarket/advancedOrders","type":"message","subject":"stopOrder","id":"6720ab1ea52a9b0001734392","userId":"66f12e8befb04d0001882b49","channelType":"private","data":{"marginMode":"ISOLATED","orderId":"240673378116083712","orderPrice":"0.1","orderType":"stop","side":"buy","size":1,"stop":"down","stopPrice":"1000","stopPriceType":"TP","symbol":"XBTUSDTM","type":"open","createdAt":1730194206837,"ts":1730194206843133000}}`

	// 3470092 Balance: the current subject and the three deprecated ones
	fxWallet            = `{"topic":"/contractAccount/wallet","type":"message","subject":"walletBalance.change","id":"67c811885b87be0001a4880e","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"crossPosMargin":"17.551016","isolatedOrderMargin":"0","holdBalance":"0","equity":"387.224858816","version":"2118","availableBalance":"285.652001096","isolatedPosMargin":"63.98342359","maxWithdrawAmount":"285.645841096","walletBalance":"371.394298816","isolatedFundingFeeMargin":"2.89220199","crossUnPnl":"2.95996","totalCrossMargin":"310.370835226","currency":"USDT","isolatedUnPnl":"12.8706","crossOrderMargin":"10.06002012","timestamp":"1741164936624"}}`
	fxWalletOrderMargin = `{"userId":"xbc453tg732eba53a88ggyt8c","topic":"/contractAccount/wallet","subject":"orderMargin.change","data":{"orderMargin":5923,"currency":"USDT","timestamp":1553842862614}}`
	fxWalletAvailable   = `{"userId":"xbc453tg732eba53a88ggyt8c","topic":"/contractAccount/wallet","subject":"availableBalance.change","data":{"availableBalance":5923,"holdBalance":2312,"currency":"USDT","timestamp":1553842862614}}`
	fxWalletWithdraw    = `{"userId":"xbc453tg732eba53a88ggyt8c","topic":"/contractAccount/wallet","subject":"withdrawHold.change","data":{"withdrawHold":5923,"currency":"USDT","timestamp":1553842862614}}`

	// 3470093 Positions: isolated change, cross change, settlement, risk limit adjustment
	fxPositionIsolated = `{"type":"message","topic":"/contract/positionAll","subject":"position.change","data":{"symbol":"XBTUSDTM","maintMarginReq":0.004,"riskLimit":50000000,"realLeverage":19.1376874933,"crossMode":false,"delevPercentage":0.87,"openingTimestamp":1771400783360,"autoDeposit":false,"currentTimestamp":1771474169458,"currentQty":-1,"currentCost":-68.0942,"currentComm":0.03794569,"unrealisedCost":-68.0942,"realisedCost":0.03794569,"isOpen":true,"markPrice":66954.6,"markValue":-66.9546,"posCost":-68.0942,"posCross":1,"posInit":1.361884,"posComm":0,"posLoss":0.00291083,"posMargin":2.35897317,"posFunding":-0.00291083,"posMaint":0.30799116,"maintMargin":3.49857317,"avgEntryPrice":68094.2,"liquidationPrice":70130.5725363,"bankruptPrice":70453.17317,"settleCurrency":"USDT","changeReason":"changeRiskLimit","riskLimitLevel":5,"realisedGrossCost":0.0,"realisedGrossPnl":0.0,"realisedPnl":-0.04376735,"unrealisedPnl":1.1396,"unrealisedPnlPcnt":0.0167,"unrealisedRoePcnt":0.8368,"leverage":19.1376874933,"marginMode":"ISOLATED","positionSide":"SHORT","tax":0,"dealComm":-0.04085652,"fundingFee":-0.00291083,"aggRate":0.0046}}`
	fxPositionCross    = `{"topic":"/contract/position:XBTUSDTM","type":"message","data":{"symbol":"XBTUSDTM","crossMode":true,"delevPercentage":0.06,"openingTimestamp":1717639498983,"currentTimestamp":1717724686618,"currentQty":-2,"currentCost":-136.002,"currentComm":0.06739824,"unrealisedCost":-136.002,"realisedCost":0.06739824,"isOpen":true,"markPrice":70778.04,"markValue":-141.55608,"posCost":-136.002,"posInit":5.4509819612,"posMargin":5.6735903779,"avgEntryPrice":68001,"liquidationPrice":80700.49720065,"bankruptPrice":81152.42267235,"settleCurrency":"USDT","changeReason":"positionChange","realisedGrossCost":0,"realisedGrossPnl":0,"realisedPnl":-0.09580416,"unrealisedPnl":-5.55408,"unrealisedPnlPcnt":-0.0408,"unrealisedRoePcnt":-1.0189,"leverage":24.95,"marginMode":"CROSS","positionSide":"BOTH"},"subject":"position.change","userId":"665ec530aa70390001d22576","channelType":"private"}`
	fxPositionSettle   = `{"type":"message","topic":"/contract/position:XBTUSDTM","subject":"position.settlement","data":{"markPrice":67198.3,"qty":-2,"positionSide":"SHORT","settleCurrency":"USDT","fundingTime":1771488000000,"marginMode":"ISOLATED","fundingFee":-0.00309113,"fundingRate":-2.3e-05,"ts":1771488018495030863}}`
	fxPositionRisk     = `{"type":"message","topic":"/contract/positionAll","subject":"position.adjustRiskLimit","data":{"msg":"Insufficient balance. Cannot increase margin.","success":false,"riskLimitLevel":9}}`

	// 3470095 Margin Mode / 3470096 Cross Margin Leverage
	fxMarginMode    = `{"topic":"/contract/marginMode","type":"message","data":{"XBTUSDTM":"CROSS"},"subject":"user.config","userId":"633559791e1cbc0001f319bc","channelType":"private"}`
	fxCrossLeverage = `{"topic":"/contract/crossLeverage","type":"message","subject":"user.config","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"XBTUSDTM":{"leverage":"51"}}}`
)
