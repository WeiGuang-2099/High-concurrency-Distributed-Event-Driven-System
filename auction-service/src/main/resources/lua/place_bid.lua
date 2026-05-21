-- KEYS[1] auction status key  (e.g. auction:42:status, value: 'ACTIVE')
-- KEYS[2] auction highest hash (fields: bidder_id, amount, bid_time)
-- ARGV[1] new bidder id    (string)
-- ARGV[2] new amount       (numeric string, e.g. '101.50')
-- ARGV[3] new bid time ms  (string)
--
-- Returns a Lua table {ok, code, prevBidder, prevAmount}
--   ok          1 = accepted, 0 = rejected
--   code        'OK' | 'AUCTION_NOT_ACTIVE' | 'BID_TOO_LOW'
--   prevBidder  previous highest bidder id, '' if none
--   prevAmount  previous highest amount, '' if none

local statusKey  = KEYS[1]
local highestKey = KEYS[2]
local newBidder  = ARGV[1]
local newAmount  = tonumber(ARGV[2])
local newTime    = ARGV[3]

local status = redis.call('GET', statusKey)
if status ~= 'ACTIVE' then
    return {0, 'AUCTION_NOT_ACTIVE', '', ''}
end

local prevAmountStr = redis.call('HGET', highestKey, 'amount')
local prevBidder = redis.call('HGET', highestKey, 'bidder_id') or ''
local currentAmount = tonumber(prevAmountStr) or 0

if newAmount <= currentAmount then
    return {0, 'BID_TOO_LOW', prevBidder, prevAmountStr or ''}
end

redis.call('HSET', highestKey,
    'bidder_id', newBidder,
    'amount',    ARGV[2],
    'bid_time',  newTime)

return {1, 'OK', prevBidder, prevAmountStr or ''}
