#!/usr/bin/env python3
import redis

def humanReadableBytes(num):
    for unit in ['','K','M']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, 'b')
        num /= 1024.0
    return "%.1f%s%s" % (num, 'G', 'b')

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost')
    keys = r.keys('*')
    count = len(keys)
    print(str(count) + ' keys found')
    currentPos = 0
    currentPercent = 0
    lastPercent = 0
    keysDebug = {'_nokey': 0}
    for key in keys:
        try:
            key = key.decode('utf-8')
            keyTree = key.split(':')
            debugInfo = r.debug_object(key)
            keySize =debugInfo['serializedlength']
            
            del keyTree[-1]
            if(len(keyTree) == 0):
                keysDebug['_nokey'] += keySize
            else:
                keyEl = keyTree[0]
                if keyEl not in keysDebug:
                    keysDebug[keyEl] = 0
                keysDebug[keyEl] += keySize
        except redis.exceptions.ResponseError:
            print('error on "' + key + '"')
        currentPos += 1
        currentPercent = currentPos / count
        if(currentPercent - lastPercent >= 0.01):
            lastPercent = round(currentPercent, 2)
            print("\r" + str(int(lastPercent * 100)) + '%', end='')
    print("\r100%")
    for keyPrefix, val in keysDebug.items():
        # val = round(int(val) / 1024 / 1024, 2)
        print(keyPrefix +' ' + humanReadableBytes(val))
