# Real market input bridge: local, disabled, no execution

`shadow_market_inputs.py` is a provider-schema decoder and disabled-by-default bounded GET transport, not a trading agent or enabled ingestion schedule. No server secrets were read, no broker/data API requests made, no files deployed, no subscription changed. Tests use mocked HTTP responses, not evidence of current account entitlement or real-time availability.

## Contract
- `parse_quote(payload, symbol, received_at=actual_receipt, now=decision_time)` outputs symbol/bid/ask/asof/received_at matching the experimental pipeline's fields. Single-symbol Alpaca quote schema (`symbol`, `quote.bp/ap/t`). Quote is at most ten seconds old; spread at most 0.2%; no crossed, zero, invalid or future prices.
- `parse_bars(payload, symbol, start=inclusive_start, end=exclusive_end, received_at=actual_receipt, now=decision_time)` outputs `Bar(symbol,start,end,close,available_at)`. `available_at` is receipt, never bar close. Every minute required in order. Missing, duplicate, partial, paginated, wrong-symbol or future input fails closed. No carry-forward or synthetic bars. Request uses 1Min/raw/IEX/asof=- and excludes inclusive endpoint's partial minute.
- For existing earnings pipeline: anchor=floor(event publication minute); endpoint=anchor+5min (plus1min if publication not exactly minute-aligned); request start=anchor−1min, exclusive end=endpoint for stock and SPY. Thus the baseline bar ends at anchor. Pipeline eligibility still independently requires trusted earnings provenance and fresh observation in its narrow window.
- `parse_session(clock,calendar,received_at,now)` needs a fresh paper clock plus today's New York calendar; regular open/close validated, including DST/early closure. Never infer opening from a fixed UTC hour or `next_open` alone.

## Explicit limitations and gates
IEX is a single-exchange observation, not consolidated NBBO or guaranteed execution price. Existing 2-minute bar lag and five-minute polling cannot meet the experimental 5–6-minute decision window; abstention is correct. Do not relabel old fetched bars as known in the past. Corporate-action-adjusted or SIP data must not silently replace raw IEX. Zero bars or 401/403/429 means unavailable, never an automatic paid upgrade or retry.

The new transport is not imported by serve/webapp/workers. Enabling it, scheduling reads and handing observations into the shadow audit require a separately reviewed integration. Actual receipt timestamp must be captured after response completion; all paired bars, quote and clock must still be fresh/complete at final decision time. Trusted event provenance remains a separate unmet gate. No submit/cancel methods exist.

## Request budget
Local hard cap 12 attempts per rolling60seconds per instance, with a lock; 5-second request timeout and 256KiB body limit; no redirect/proxy, provider-error body, pagination-following or retries. One minimal candidate sample = stock bars + SPY bars + stock quote + clock + today's calendar (5 GETs); calendar can be cached per New York date only by a reviewed caller. This is not an account-wide limiter: before scheduling multiple processes, use a shared budget accounting for existing workers too. Published Basic historical quota is200/minute, not a performance guarantee and not permission to consume it all.

## Official references verified 2026-10-19
- https://docs.alpaca.markets/us/docs/about-market-data-api : Basic free IEX coverage and historical call quota. Broad historical limitation table is not proof this account has recent-bar access; entitlement/freshness still require actual observed responses.
- https://docs.alpaca.markets/us/docs/market-data-faq : single exchange versus SIP; recent SIP entitlement restrictions.
- https://docs.alpaca.markets/us/reference/stockbars : inclusive endpoints, raw adjustment, feed, pagination and global page limits.
- https://docs.alpaca.markets/us/v1.4.2/reference/stocklatestquotesingle-1 : native single-symbol quote route.
- https://docs.alpaca.markets/us/v1.4.2/reference/getclock-1 : current timestamp, current openness and next boundaries.
- https://docs.alpaca.markets/us/v1.4.2/reference/getcalendar-1 : daily session times including early closes.
