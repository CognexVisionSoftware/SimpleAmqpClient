#ifndef SIMPLEAMQPCLIENT_BYTES_H
#define SIMPLEAMQPCLIENT_BYTES_H

#include <amqp.h>

#include <string>
#include <string_view>

namespace AmqpClient {

amqp_bytes_t StringToBytes(const std::string& str) {
  amqp_bytes_t ret;
  ret.bytes = reinterpret_cast<void*>(const_cast<char*>(str.data()));
  ret.len = str.length();
  return ret;
}

amqp_bytes_t StringRefToBytes(std::string_view str) {
  amqp_bytes_t ret;
  ret.bytes = reinterpret_cast<void*>(const_cast<char*>(str.data()));
  ret.len = str.length();
  return ret;
}

}  // namespace AmqpClient
#endif  // SIMPLEAMQPCLIENT_BYTES_H
