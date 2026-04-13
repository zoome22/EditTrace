#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import base64
import hashlib
import hmac
from Crypto.Cipher import AES


_KEY = b'\x16\x08\x09\x6f\x02\x17\x2b\x08\x21\x21\x0a\x10\x03\x03\x07\x06'
_IV  = b'\x0f\x08\x01\x00\x19\x47\x25\xdc\x15\xf5\x17\xe0\xe1\x15\x0c\x35'
key_cache = {}

class android_decrypter :
	def __init__(self) :
		self.key = _KEY
		self.iv = _IV
		
	def incept(self, n):
		dict1 = ['adrp.ldrsh.ldnp', 'ldpsw', 'umax', 'stnp.rsubhn', 'sqdmlsl', 'uqrshl.csel', 'sqshlu', 'umin.usubl.umlsl', 'cbnz.adds', 'tbnz',
				 'usubl2', 'stxr', 'sbfx', 'strh', 'stxrb.adcs', 'stxrh', 'ands.urhadd', 'subs', 'sbcs', 'fnmadd.ldxrb.saddl',
				 'stur', 'ldrsb', 'strb', 'prfm', 'ubfiz', 'ldrsw.madd.msub.sturb.ldursb', 'ldrb', 'b.eq', 'ldur.sbfiz', 'extr',
				 'fmadd', 'uqadd', 'sshr.uzp1.sttrb', 'umlsl2', 'rsubhn2.ldrh.uqsub', 'uqshl', 'uabd', 'ursra', 'usubw', 'uaddl2',
				 'b.gt', 'b.lt', 'sqshl', 'bics', 'smin.ubfx', 'smlsl2', 'uabdl2', 'zip2.ssubw2', 'ccmp', 'sqdmlal',
				 'b.al', 'smax.ldurh.uhsub', 'fcvtxn2', 'b.pl']
		dict2 = ['saddl', 'urhadd', 'ubfiz.sqdmlsl.tbnz.stnp', 'smin', 'strh', 'ccmp', 'usubl', 'umlsl', 'uzp1', 'sbfx',
				 'b.eq', 'zip2.prfm.strb', 'msub', 'b.pl', 'csel', 'stxrh.ldxrb', 'uqrshl.ldrh', 'cbnz', 'ursra', 'sshr.ubfx.ldur.ldnp',
				 'fcvtxn2', 'usubl2', 'uaddl2', 'b.al', 'ssubw2', 'umax', 'b.lt', 'adrp.sturb', 'extr', 'uqshl',
				 'smax', 'uqsub.sqshlu', 'ands', 'madd', 'umin', 'b.gt', 'uabdl2', 'ldrsb.ldpsw.rsubhn', 'uqadd', 'sttrb',
				 'stxr', 'adds', 'rsubhn2.umlsl2', 'sbcs.fmadd', 'usubw', 'sqshl', 'stur.ldrsh.smlsl2', 'ldrsw', 'fnmadd', 'stxrb.sbfiz',
				 'adcs', 'bics.ldrb', 'l1ursb', 'subs.uhsub', 'ldurh', 'uabd', 'sqdmlal']
		word1 = dict1[  n	 % len(dict1) ]
		word2 = dict2[ (n+31) % len(dict2) ]
		return word1 + '.' + word2

	def genSalt(self,user_id, encType):
		if user_id <= 0:
			return b'\0' * 16
		prefixes = [
			'','', '12','24','18','30','36','12','48','7','35','40','17','23','29',
			'isabel','kale','sulli','van','merry','kyle','james','maddux',
			'tony','hayden','paul','elijah','dorothy','sally','bran',
			self.incept(830819), 'veil'
		]
		if encType < 0 or encType >= len(prefixes):
			raise ValueError(f'Unsupported encoding type {encType}')
		salt = (prefixes[encType] + str(user_id))[:16]
		salt = (salt + '\0' * (16 - len(salt))).encode('utf-8')
		return salt

	def pkcs16adjust(self, a, aOff, b):
		x = (b[len(b) - 1] & 0xff) + (a[aOff + len(b) - 1] & 0xff) + 1
		a[aOff + len(b) - 1] = x % 256
		x >>= 8
		for i in range(len(b)-2, -1, -1):
			x = x + (b[i] & 0xff) + (a[aOff + i] & 0xff)
			a[aOff + i] = x % 256
			x >>= 8

	def deriveKey(self, password, salt, iterations, dkeySize):
		password = (password + b'\0').decode('ascii').encode('utf-16-be')
		hasher = hashlib.sha1()
		v = hasher.block_size
		u = hasher.digest_size

		D = [1] * v
		S = [0] * (v * ((len(salt) + v - 1) // v))
		for i in range(len(S)):
			S[i] = salt[i % len(salt)]
		P = [0] * (v * ((len(password) + v - 1) // v))
		for i in range(len(P)):
			P[i] = password[i % len(password)]

		I = S + P
		B = [0] * v
		c = (dkeySize + u - 1) // u

		dKey = [0] * dkeySize
		for i in range(1, c + 1):
			hasher = hashlib.sha1()
			hasher.update(bytes(D))
			hasher.update(bytes(I))
			A = hasher.digest()

			for _ in range(1, iterations):
				hasher = hashlib.sha1()
				hasher.update(A)
				A = hasher.digest()

			A = list(A)
			for j in range(len(B)):
				B[j] = A[j % len(A)]

			for j in range(0, len(I) // v):
				self.pkcs16adjust(I, j * v, B)

			start = (i - 1) * u
			if i == c:
				dKey[start:dkeySize] = A[0:dkeySize - start]
			else:
				dKey[start:start + len(A)] = A[0:len(A)]

		return bytes(dKey)

	def _derive_aes_key(self, user_id, encType):
		# Hardcoded base key & IV from original code
		base_key = b'\x16\x08\x09\x6f\x02\x17\x2b\x08\x21\x21\x0a\x10\x03\x03\x07\x06'
		iv = b'\x0f\x08\x01\x00\x19\x47\x25\xdc\x15\xf5\x17\xe0\xe1\x15\x0c\x35'
		salt = self.genSalt(user_id, encType)
		cache_key = (salt, )
		if cache_key in key_cache:
			key = key_cache[cache_key]
		else:
			key = self.deriveKey(base_key, salt, 2, 32)
			key_cache[cache_key] = key
		return key, iv

	def _valid_pkcs7(self, data):
		if not data:
			return False
		pad = data[-1]
		if pad < 1 or pad > 16:
			return False
		if len(data) < pad:
			return False
		return data.endswith(bytes([pad]) * pad)

	def decrypt_with_enc(self, user_id, encType, b64_ciphertext):
		key, iv = self._derive_aes_key(user_id, encType)
		cipher = AES.new(key, AES.MODE_CBC, iv)
		ct = base64.b64decode(b64_ciphertext)
		if len(ct) == 0:
			# nothing to do; return original
			return None
		padded = cipher.decrypt(ct)
		if not self._valid_pkcs7(padded):
			return None
		plaintext = padded[:-padded[-1]]
		try:
			text = plaintext.decode('utf-8')
			return text
		except UnicodeDecodeError:
			# return bytes if not utf-8, still counts as success
			return plaintext

	def decrypt_try_all(self, user_id, b64_ciphertext):
		# Try all encTypes supported by genSalt() prefixes (0..len-1)
		max_type = 32  # based on prefixes length in genSalt (index 0..31)
		best = None
		for encType in range(max_type):
			try:
				out = self.decrypt_with_enc(user_id, encType, b64_ciphertext)
			except ValueError:
				out = None
			if out is None:
				continue
			# Prefer valid UTF-8 text
			if isinstance(out, str):
				return encType, out
			# Otherwise keep first bytes result as fallback
			if best is None:
				best = (encType, out)
		return best  # may be None if nothing worked
