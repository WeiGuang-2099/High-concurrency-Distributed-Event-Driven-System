-- KEYS[1] auction status key  (e.g. auction:42:status, value: 'ACTIVE')
-- KEYS[2] auction highest hash (fields: bidder_id, amount, bid_time)
-- KEYS[3] bid outbox stream   (e.g. auction:bid:outbox)
-- ARGV[1] new bidder id       (string)
-- ARGV[2] new amount          (numeric string, e.g. '101.50')
-- ARGV[3] new bid time ms     (string)
-- ARGV[4] auction id          (string)
-- ARGV[5] new bidder username (string)
-- ARGV[6] outbox maxlen       (string, approximate trim bound)
--
-- Returns a Lua table {ok, code, prevBidder, prevAmount, streamId}
--   ok          1 = accepted, 0 = rejected
--   code        'OK' | 'AUCTION_NOT_ACTIVE' | 'BID_TOO_LOW'
--   prevBidder  previous highest bidder id, '' if none
--   prevAmount  previous highest amount, '' if none
--   streamId    outbox entry id when accepted, '' otherwise
--
-- Accepting a bid and recording it durably happen inside this one script, so they
-- are atomic: there is no window where the caller is told 201 but the bid exists
-- only in the highest hash and never reaches Kafka. A relay drains the stream.
-- This is the outbox pattern with Redis as the source of truth -- the textbook
-- "insert the event in the same DB transaction" does not apply, because the accept
-- decision is made here and never touches MySQL on the hot path.
--
-- Single-node Redis only: the keys share no hash tag, so this would be rejected in
-- cluster mode (as it already would have been with two keys).

local statusKey  = KEYS[1]
local highestKey = KEYS[2]
local outboxKey  = KEYS[3]
local newBidder  = ARGV[1]
local newAmount  = tonumber(ARGV[2])
local newTime    = ARGV[3]
local auctionId  = ARGV[4]
local newUser    = ARGV[5]
local maxLen     = ARGV[6]

local status = redis.call('GET', statusKey)
if status ~= 'ACTIVE' then
    return {0, 'AUCTION_NOT_ACTIVE', '', '', ''}
end

local prevAmountStr = redis.call('HGET', highestKey, 'amount')
local prevBidder = redis.call('HGET', highestKey, 'bidder_id') or ''
local currentAmount = tonumber(prevAmountStr) or 0

if newAmount <= currentAmount then
    return {0, 'BID_TOO_LOW', prevBidder, prevAmountStr or '', ''}
end

redis.call('HSET', highestKey,
    'bidder_id', newBidder,
    'amount',    ARGV[2],
    'bid_time',  newTime)

-- MAXLEN ~ bounds the stream so a stuck relay cannot exhaust memory. Approximate
-- trimming drops whole nodes from the head, so it only discards entries once the
-- relay has fallen roughly maxLen behind -- by then events are being lost and relay
-- lag is the real alarm.
local streamId = redis.call('XADD', outboxKey, 'MAXLEN', '~', maxLen, '*',
    'auctionId',      auctionId,
    'bidderId',       newBidder,
    'bidderUsername', newUser,
    'amount',         ARGV[2],
    'bidTime',        newTime,
    'prevBidder',     prevBidder,
    'prevAmount',     prevAmountStr or '')

return {1, 'OK', prevBidder, prevAmountStr or '', streamId}
