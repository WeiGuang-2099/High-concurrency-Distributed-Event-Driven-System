-- KEYS[1] stock key (e.g. stock:1001:VIP)
-- ARGV[1] quantity to release
--
-- Returns {1, 'OK'}

local stockKey = KEYS[1]
local quantity = tonumber(ARGV[1])

redis.call('INCRBY', stockKey, quantity)
return {1, 'OK'}
