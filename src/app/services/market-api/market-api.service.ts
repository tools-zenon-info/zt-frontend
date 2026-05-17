import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import {
    interval,
    map,
    Observable,
    pluck,
    shareReplay,
    startWith,
    switchMap,
} from 'rxjs';
import Common from 'src/app/utils/common';
import { environment } from 'src/environments/environment';
import { CoinGeckoResponse, MarketChart, MarketData } from './coin-gecko-types';

@Injectable({
    providedIn: 'root',
})
export class MarketApiService {
    private baseUrl = environment.coinGeckoUrl;
    private currentPriceIntervalMs = 1000 * 60 * 5;
    private historyIntervalMs = 1000 * 60 * 10;

    private znnHistory$ = this.getCoinHistory('zenon-2', 'usd');
    private qsrHistory$ = this.getCoinHistory('quasar', 'usd');

    public currentZnnPrice$ = this.getCurrentCoinPrice('zenon-2');
    public znnPriceHistory7d$ = this.getCoinPriceHistory(this.znnHistory$);
    public znnTradingVolume7d$ = this.getCoinTradingVolume(this.znnHistory$);
    public qsrPriceHistory7d$ = this.getCoinPriceHistory(this.qsrHistory$);
    public qsrTradingVolume7d$ = this.getCoinTradingVolume(this.qsrHistory$);

    constructor(private httpClient: HttpClient) {}

    private get coinGeckoHeaders(): { [key: string]: string } {
        // CoinGecko Demo API key: per-key quota (~30/min), bypasses IP-based
        // rate limits. Header is the documented way to authenticate.
        return environment.coinGeckoApiKey
            ? { 'x-cg-demo-api-key': environment.coinGeckoApiKey }
            : {};
    }

    private getCurrentCoinPrice(coin: string) {
        return interval(this.currentPriceIntervalMs).pipe(
            startWith(0),
            Common.whenPageVisible(),
            switchMap(() =>
                this.httpClient
                    .get<CoinGeckoResponse>(`${this.baseUrl}/coins/${coin}`, { headers: this.coinGeckoHeaders })
                    .pipe(pluck('market_data', 'current_price'))
            ),
            shareReplay(1)
        );
    }

    private getCoinPriceHistory(coinHistory: Observable<MarketChart>) {
        return interval(this.historyIntervalMs).pipe(
            startWith(0),
            Common.whenPageVisible(),
            switchMap(() =>
                coinHistory.pipe(
                    map((response) => response.prices.map((i) => i[1]))
                )
            )
        );
    }

    private getCoinTradingVolume(coinHistory: Observable<MarketChart>) {
        return coinHistory.pipe(
            Common.whenPageVisible(),
            map((response) => response.total_volumes.map((i) => i[1]))
        );
    }

    private getCoinHistory(coin: string, currency: string) {
        const historyDataUrl = new URL(
            `${this.baseUrl}/coins/${coin}/market_chart`
        );
        [
            ['vs_currency', currency],
            ['days', '7'],
            ['interval', 'daily'],
        ].forEach(([key, value]) =>
            historyDataUrl.searchParams.set(key, value)
        );
        return this.httpClient
            .get<MarketChart>(historyDataUrl.toString(), { headers: this.coinGeckoHeaders })
            .pipe(shareReplay(1));
    }
}
