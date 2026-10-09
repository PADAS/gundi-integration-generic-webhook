"""In-memory stand-in for the async Redis calls the outbound modules make.

Values come back as bytes, like redis-py without decode_responses. eval()
emulates the Lua scripts in app/services/outbound_buffer.py by what they are
meant to do; the Lua itself is not run, so a change to the scripts is not
covered by these tests. TTLs are recorded, not enforced.
"""
from app.services import outbound_buffer


def _bytes(value):
    if isinstance(value, bytes):
        return value
    return str(value).encode()


class FakeRedis:

    def __init__(self):
        self.values = {}
        self.lists = {}
        self.ttls = {}

    async def get(self, name):
        return self.values.get(name)

    async def set(self, name, value, nx=False, ex=None):
        if nx and name in self.values:
            return None
        self.values[name] = _bytes(value)
        self.ttls[name] = ex
        return True

    async def setex(self, name, time, value):
        self.values[name] = _bytes(value)
        self.ttls[name] = time
        return True

    async def delete(self, *names):
        return sum(1 for name in names if self.values.pop(name, None) is not None)

    async def incrby(self, name, amount):
        value = int(self.values.get(name, b"0")) + amount
        self.values[name] = _bytes(value)
        return value

    async def incr(self, name):
        return await self.incrby(name, 1)

    async def decrby(self, name, amount):
        return await self.incrby(name, -amount)

    async def expire(self, name, time):
        self.ttls[name] = time
        return name in self.values

    async def rpush(self, name, *values):
        items = self.lists.setdefault(name, [])
        items.extend(_bytes(v) for v in values)
        return len(items)

    async def llen(self, name):
        return len(self.lists.get(name, []))

    async def lindex(self, name, index):
        items = self.lists.get(name, [])
        return items[index] if -len(items) <= index < len(items) else None

    async def lrange(self, name, start, end):
        items = self.lists.get(name, [])
        return items[start:] if end == -1 else items[start:end + 1]

    async def ltrim(self, name, start, end):
        items = self.lists.get(name, [])
        self.lists[name] = items[start:] if end == -1 else items[start:end + 1]
        return True

    async def eval(self, script, numkeys, *keys_and_args):
        keys, args = keys_and_args[:numkeys], keys_and_args[numkeys:]
        if script == outbound_buffer._PUSH:
            buffer, lock, dropped = keys
            length = await self.rpush(buffer, args[0])
            excess = length - int(args[1])
            if excess > 0 and lock not in self.values:
                await self.ltrim(buffer, excess, -1)
                await self.incrby(dropped, excess)
                return [length - excess, excess]
            return [length, 0]
        holds_lock = self.values.get(keys[0]) == _bytes(args[0])
        if script == outbound_buffer._TRIM_IF_LOCKED:
            if not holds_lock:
                return 0
            await self.ltrim(keys[1], int(args[1]), -1)
            return 1
        if script == outbound_buffer._RELEASE_AND_CAP:
            if not holds_lock:
                return 0
            excess = len(self.lists.get(keys[1], [])) - int(args[1])
            if excess > 0:
                await self.ltrim(keys[1], excess, -1)
                await self.incrby(keys[2], excess)
            await self.delete(keys[0])
            return max(excess, 0)
        raise NotImplementedError("FakeRedis does not emulate this script")
