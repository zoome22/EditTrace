import sys
import sqlite3
import hmac
import base64
import hashlib
from Crypto.Cipher import AES
from datetime import datetime

key = b'\x16\x08\x09\x6f\x02\x17\x2b\x08\x21\x21\x0a\x10\x03\x03\x07\x06'
iv  = b'\x0f\x08\x01\x00\x19\x47\x25\xdc\x15\xf5\x17\xe0\xe1\x15\x0c\x35'

class ios_decrypter :
	def __init__(self) :
		self.key = key
		self.iv = iv

	def humantime(self, temp) :
		unix = datetime(1970, 1, 1)  # UTC
		cocoa = datetime(2001, 1, 1)  # UTC
		delta = cocoa - unix  # timedelta instance
		timestamp = datetime.fromtimestamp(int(temp)) + delta
		time = (timestamp.strftime('%Y-%m-%d %H:%M:%S'))
		return time 

	def calcHash(self, msg):
		first  = hmac.new(key, msg,   hashlib.sha1).digest()
		second = hmac.new(key, first, hashlib.sha1).digest()
		return bytes(map(lambda x: x[0] ^ x[1], zip(first, second)))

	def deriveKey(self, userId):
		userId = str(userId) + '\0' * 16
		userId = userId[:16].encode('utf-8')
		first  = self.calcHash(userId + b'\x00\x00\x00\x01')
		second = self.calcHash(userId + b'\x00\x00\x00\x02')
		return (first + second)[:32]

	def decrypt(self, userId, msg):
		try :
			encoder = AES.new(self.deriveKey(userId), AES.MODE_CBC, iv)
			decrypted = encoder.decrypt(base64.b64decode(msg))
			return decrypted[:-decrypted[-1]].decode('utf-8')
		except :
			return str("===ERROR_KAKAOTALK_DECRYPTION===")