/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2019 Telegram Systems LLP
*/
#include "EncryptedKey.h"

#include "tonlib/keys/DecryptedKey.h"
#include "tonlib/keys/SimpleEncryption.h"

#include "td/utils/crypto.h"

namespace tonlib {
td::Result<DecryptedKey> EncryptedKey::decrypt(td::Slice local_password, bool check_public_key, bool old) const {
  if (secret.size() != 32) {
    return td::Status::Error("Failed to decrypt key: invalid secret size");
  }
  td::SecureString decrypted_secret(32);
  if (old) {
    td::SecureString local_password_hash(32);
    sha256(local_password, local_password_hash.as_mutable_slice());
    for (size_t i = 0; i < 32; i++) {
      decrypted_secret.as_mutable_slice()[i] = secret.as_slice()[i] ^ local_password_hash.as_slice()[i];
    }
  } else {
    hmac_sha256(secret, local_password, decrypted_secret.as_mutable_slice());
  }

  td::SecureString encryption_secret(64);
  pbkdf2_sha512(as_slice(decrypted_secret), "TON local key", PBKDF_ITERATIONS, encryption_secret.as_mutable_slice());
  TRY_RESULT(decrypted_data, SimpleEncryption::decrypt_data(as_slice(encrypted_data), as_slice(encryption_secret)));

  RawDecryptedKey raw_decrypted_key;
  TRY_STATUS(td::unserialize(raw_decrypted_key, decrypted_data));
  DecryptedKey res(std::move(raw_decrypted_key));
  TRY_RESULT(got_public_key, res.private_key.get_public_key());
  if (check_public_key &&
      got_public_key.as_octet_string().as_slice() != this->public_key.as_octet_string().as_slice()) {
    return td::Status::Error("Something wrong: public key of decrypted private key differs from requested public key");
  }
  return std::move(res);
}
}  // namespace tonlib
