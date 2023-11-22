/*
 * ***** BEGIN LICENSE BLOCK *****
 * Version: MIT
 *
 * Copyright (c) 2010-2013 Alan Antonuk
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ***** END LICENSE BLOCK *****
 */

#ifdef _WIN32
#define NOMINMAX
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <Winsock2.h>
#else
#include <sys/time.h>
#include <sys/types.h>
#endif

#include <string.h>

#include <array>
#include <cassert>
#include <chrono>
#include <string>

#include "SimpleAmqpClient/AmqpException.h"
#include "SimpleAmqpClient/AmqpLibraryException.h"
#include "SimpleAmqpClient/AmqpResponseLibraryException.h"
#include "SimpleAmqpClient/ChannelImpl.h"
#include "SimpleAmqpClient/ConnectionClosedException.h"
#include "SimpleAmqpClient/ConsumerTagNotFoundException.h"
#include "SimpleAmqpClient/TableImpl.h"
#include "SimpleAmqpClient/Bytes.h"
#include "SimpleAmqpClient/MessageRejectedException.h"
#include "SimpleAmqpClient/MessageReturnedException.h"

#define BROKER_HEARTBEAT 0

namespace AmqpClient {

namespace {

std::string BytesToString(amqp_bytes_t bytes) {
  return std::string(reinterpret_cast<char *>(bytes.bytes), bytes.len);
}

void SetMessageProperties(BasicMessage &mes,
                          const amqp_basic_properties_t &props) {
  if (0 != (props._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)) {
    mes.ContentType(BytesToString(props.content_type));
  }
  if (0 != (props._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG)) {
    mes.ContentEncoding(BytesToString(props.content_encoding));
  }
  if (0 != (props._flags & AMQP_BASIC_DELIVERY_MODE_FLAG)) {
    mes.DeliveryMode(
        static_cast<BasicMessage::delivery_mode_t>(props.delivery_mode));
  }
  if (0 != (props._flags & AMQP_BASIC_PRIORITY_FLAG)) {
    mes.Priority(props.priority);
  }
  if (0 != (props._flags & AMQP_BASIC_CORRELATION_ID_FLAG)) {
    mes.CorrelationId(BytesToString(props.correlation_id));
  }
  if (0 != (props._flags & AMQP_BASIC_REPLY_TO_FLAG)) {
    mes.ReplyTo(BytesToString(props.reply_to));
  }
  if (0 != (props._flags & AMQP_BASIC_EXPIRATION_FLAG)) {
    mes.Expiration(BytesToString(props.expiration));
  }
  if (0 != (props._flags & AMQP_BASIC_MESSAGE_ID_FLAG)) {
    mes.MessageId(BytesToString(props.message_id));
  }
  if (0 != (props._flags & AMQP_BASIC_TIMESTAMP_FLAG)) {
    mes.Timestamp(props.timestamp);
  }
  if (0 != (props._flags & AMQP_BASIC_TYPE_FLAG)) {
    mes.Type(BytesToString(props.type));
  }
  if (0 != (props._flags & AMQP_BASIC_USER_ID_FLAG)) {
    mes.UserId(BytesToString(props.user_id));
  }
  if (0 != (props._flags & AMQP_BASIC_APP_ID_FLAG)) {
    mes.AppId(BytesToString(props.app_id));
  }
  if (0 != (props._flags & AMQP_BASIC_CLUSTER_ID_FLAG)) {
    mes.ClusterId(BytesToString(props.cluster_id));
  }
  if (0 != (props._flags & AMQP_BASIC_HEADERS_FLAG)) {
    mes.HeaderTable(Detail::TableValueImpl::CreateTable(props.headers));
  }
}
}  // namespace

Channel::ChannelImpl::ChannelImpl()
    : m_last_used_channel(0), m_is_connected(false) {
  m_channels.push_back({CS_Used});
}

Channel::ChannelImpl::~ChannelImpl() {}

void Channel::ChannelImpl::DoLogin(const std::string &username,
                                   const std::string &password,
                                   const std::string &vhost, int frame_max,
                                   bool sasl_external) {
  amqp_table_entry_t capabilties[1];
  amqp_table_entry_t capability_entry;
  amqp_table_t client_properties;

  capabilties[0].key = amqp_cstring_bytes("consumer_cancel_notify");
  capabilties[0].value.kind = AMQP_FIELD_KIND_BOOLEAN;
  capabilties[0].value.value.boolean = 1;

  capability_entry.key = amqp_cstring_bytes("capabilities");
  capability_entry.value.kind = AMQP_FIELD_KIND_TABLE;
  capability_entry.value.value.table.num_entries =
      sizeof(capabilties) / sizeof(amqp_table_entry_t);
  capability_entry.value.value.table.entries = capabilties;

  client_properties.num_entries = 1;
  client_properties.entries = &capability_entry;

  if (sasl_external) {
    CheckRpcReply(0, amqp_login_with_properties(
                         m_connection, vhost.c_str(), 0, frame_max,
                         BROKER_HEARTBEAT, &client_properties,
                         AMQP_SASL_METHOD_EXTERNAL, username.c_str()));
  } else {
    CheckRpcReply(
        0, amqp_login_with_properties(m_connection, vhost.c_str(), 0, frame_max,
                                      BROKER_HEARTBEAT, &client_properties,
                                      AMQP_SASL_METHOD_PLAIN, username.c_str(),
                                      password.c_str()));
  }

  m_brokerVersion = ComputeBrokerVersion(m_connection);
}

amqp_channel_t Channel::ChannelImpl::GetNextChannelId() {
  channel_state_list_t::iterator unused_channel =
      std::find_if(m_channels.begin(), m_channels.end(), [] (const channel_state_t& state) {
        return state.availability == CS_Closed;
      });

  if (m_channels.end() == unused_channel) {
    int max_channels = amqp_get_channel_max(m_connection);
    if (0 == max_channels) {
      max_channels = std::numeric_limits<uint16_t>::max();
    }
    if (static_cast<size_t>(max_channels) < m_channels.size()) {
      throw std::runtime_error("Too many channels open");
    }

    m_channels.push_back({CS_Closed});
    unused_channel = m_channels.end() - 1;
  }

  return unused_channel - m_channels.begin();
}

amqp_channel_t Channel::ChannelImpl::CreateNewChannel() {
  amqp_channel_t new_channel = GetNextChannelId();

  static const std::array<std::uint32_t, 1> OPEN_OK = {
      AMQP_CHANNEL_OPEN_OK_METHOD};
  amqp_channel_open_t channel_open = {};
  DoRpcOnChannel<std::array<std::uint32_t, 1> >(
      new_channel, AMQP_CHANNEL_OPEN_METHOD, &channel_open, OPEN_OK);

  static const std::array<std::uint32_t, 1> CONFIRM_OK = {
      AMQP_CONFIRM_SELECT_OK_METHOD};
  amqp_confirm_select_t confirm_select = {};
  DoRpcOnChannel<std::array<std::uint32_t, 1> >(
      new_channel, AMQP_CONFIRM_SELECT_METHOD, &confirm_select, CONFIRM_OK);

  m_channels.at(new_channel).availability = CS_Open;

  return new_channel;
}

amqp_channel_t Channel::ChannelImpl::GetChannel() {
  if (CS_Open == m_channels.at(m_last_used_channel).availability) {
    m_channels[m_last_used_channel].availability = CS_Used;
    return m_last_used_channel;
  }

  channel_state_list_t::iterator it =
      std::find_if(m_channels.begin(), m_channels.end(), [] (const channel_state_t& state) {
        return state.availability == CS_Open;
      });

  if (m_channels.end() == it) {
    amqp_channel_t new_channel = CreateNewChannel();
    m_channels.at(new_channel).availability = CS_Used;
    return new_channel;
  }

  it->availability = CS_Used;
  return it - m_channels.begin();
}

void Channel::ChannelImpl::ReturnChannel(amqp_channel_t channel) {
  m_channels.at(channel).availability = CS_Open;
  m_last_used_channel = channel;
}

bool Channel::ChannelImpl::IsChannelOpen(amqp_channel_t channel) {
  return CS_Closed != m_channels.at(channel).availability;
}

void Channel::ChannelImpl::FinishCloseChannel(amqp_channel_t channel) {
  m_channels.at(channel).availability = CS_Closed;

  amqp_channel_close_ok_t close_ok;
  CheckForError(amqp_send_method(m_connection, channel,
                                 AMQP_CHANNEL_CLOSE_OK_METHOD, &close_ok));
}

void Channel::ChannelImpl::FinishCloseConnection() {
  SetIsConnected(false);
  amqp_connection_close_ok_t close_ok;
  amqp_send_method(m_connection, 0, AMQP_CONNECTION_CLOSE_OK_METHOD, &close_ok);
}

void Channel::ChannelImpl::CheckRpcReply(amqp_channel_t channel,
                                         const amqp_rpc_reply_t &reply) {
  switch (reply.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return;
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      // If we're getting this likely is the socket is already closed
      throw AmqpResponseLibraryException::CreateException(reply, "");
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
        FinishCloseChannel(channel);
      } else if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
        FinishCloseConnection();
      }
      AmqpException::Throw(reply);
      break;

    default:
      AmqpException::Throw(reply);
  }
}

void Channel::ChannelImpl::CheckForError(int ret) {
  if (ret < 0) {
    throw AmqpLibraryException::CreateException(ret);
  }
}

MessageReturnedException Channel::ChannelImpl::CreateMessageReturnedException(
    amqp_basic_return_t &return_method, amqp_channel_t channel) {
  const int reply_code = return_method.reply_code;
  const std::string reply_text((char *)return_method.reply_text.bytes,
                               return_method.reply_text.len);
  const std::string exchange((char *)return_method.exchange.bytes,
                             return_method.exchange.len);
  const std::string routing_key((char *)return_method.routing_key.bytes,
                                return_method.routing_key.len);
  BasicMessage::ptr_t content = ReadContent(channel);
  return MessageReturnedException(content, reply_code, reply_text, exchange,
                                  routing_key);
}

BasicMessage::ptr_t Channel::ChannelImpl::ReadContent(amqp_channel_t channel) {
  amqp_frame_t frame;

  GetNextFrameOnChannel(channel, frame);

  if (frame.frame_type != AMQP_FRAME_HEADER) {
    // TODO: We should connection.close here
    throw std::runtime_error(
        "Channel::BasicConsumeMessage: received unexpected frame type (was "
        "expected AMQP_FRAME_HEADER)");
  }

  // The memory for this is allocated in a pool associated with the connection
  // The BasicMessage constructor does a deep copy of the properties structure
  amqp_basic_properties_t *properties =
      reinterpret_cast<amqp_basic_properties_t *>(
          frame.payload.properties.decoded);

  // size_t could possibly be 32-bit, body_size is always 64-bit
  assert(frame.payload.properties.body_size <
         static_cast<uint64_t>(std::numeric_limits<size_t>::max()));

  size_t body_size = static_cast<size_t>(frame.payload.properties.body_size);
  size_t received_size = 0;

  BasicMessage::ptr_t message = BasicMessage::Create();
  message->Body().reserve(body_size);

  // frame #3 and up:
  while (received_size < body_size) {
    GetNextFrameOnChannel(channel, frame);

    if (frame.frame_type != AMQP_FRAME_BODY)
      // TODO: we should connection.close here
      throw std::runtime_error(
          "Channel::BasicConsumeMessage: received unexpected frame type (was "
          "expecting AMQP_FRAME_BODY)");

    message->Body().append(
        reinterpret_cast<char *>(frame.payload.body_fragment.bytes),
        frame.payload.body_fragment.len);
    received_size += frame.payload.body_fragment.len;
  }

  SetMessageProperties(*message, *properties);

  return message;
}

void Channel::ChannelImpl::CheckFrameForClose(amqp_frame_t &frame,
                                              amqp_channel_t channel) {
  if (frame.frame_type == AMQP_FRAME_METHOD) {
    switch (frame.payload.method.id) {
      case AMQP_CHANNEL_CLOSE_METHOD:
        FinishCloseChannel(channel);
        AmqpException::Throw(*reinterpret_cast<amqp_channel_close_t *>(
            frame.payload.method.decoded));
        break;

      case AMQP_CONNECTION_CLOSE_METHOD:
        FinishCloseConnection();
        AmqpException::Throw(*reinterpret_cast<amqp_connection_close_t *>(
            frame.payload.method.decoded));
        break;
    }
  }
}

void Channel::ChannelImpl::AddConsumer(const std::string &consumer_tag,
                                       amqp_channel_t channel) {
  m_consumer_channel_map.insert(std::make_pair(consumer_tag, channel));
}

amqp_channel_t Channel::ChannelImpl::RemoveConsumer(
    const std::string &consumer_tag) {
  std::map<std::string, amqp_channel_t>::iterator it =
      m_consumer_channel_map.find(consumer_tag);
  if (it == m_consumer_channel_map.end()) {
    throw ConsumerTagNotFoundException();
  }

  amqp_channel_t result = it->second;

  m_consumer_channel_map.erase(it);

  return result;
}

amqp_channel_t Channel::ChannelImpl::GetConsumerChannel(
    const std::string &consumer_tag) {
  std::map<std::string, amqp_channel_t>::const_iterator it =
      m_consumer_channel_map.find(consumer_tag);
  if (it == m_consumer_channel_map.end()) {
    throw ConsumerTagNotFoundException();
  }
  return it->second;
}

std::vector<amqp_channel_t> Channel::ChannelImpl::GetAllConsumerChannels()
    const {
  std::vector<amqp_channel_t> ret;
  for (consumer_map_t::const_iterator it = m_consumer_channel_map.begin();
       it != m_consumer_channel_map.end(); ++it) {
    ret.push_back(it->second);
  }

  return ret;
}

bool Channel::ChannelImpl::CheckForQueuedMessageOnChannel(
    amqp_channel_t channel) const {
  frame_queue_t::const_iterator it = std::find_if(
      m_frame_queue.begin(), m_frame_queue.end(), [channel](auto &frame) {
        return ChannelImpl::is_method_on_channel(
            frame, AMQP_BASIC_DELIVER_METHOD, channel);
      });

  if (it == m_frame_queue.end()) {
    return false;
  }

  it = std::find_if(it + 1, m_frame_queue.end(), [channel](auto &frame) {
    return Channel::ChannelImpl::is_on_channel(frame, channel);
  });

  if (it == m_frame_queue.end()) {
    return false;
  }
  if (it->frame_type != AMQP_FRAME_HEADER) {
    throw std::runtime_error("Protocol error");
  }

  uint64_t body_length = it->payload.properties.body_size;
  uint64_t body_received = 0;

  while (body_received < body_length) {
    it = std::find_if(it + 1, m_frame_queue.end(), [channel](auto &frame) {
      return Channel::ChannelImpl::is_on_channel(frame, channel);
    });

    if (it == m_frame_queue.end()) {
      return false;
    }
    if (it->frame_type != AMQP_FRAME_BODY) {
      throw std::runtime_error("Protocol error");
    }
    body_received += it->payload.body_fragment.len;
  }

  return true;
}

void Channel::ChannelImpl::AddToFrameQueue(const amqp_frame_t &frame) {
  m_frame_queue.push_back(frame);

  if (CheckForQueuedMessageOnChannel(frame.channel)) {
    std::array<amqp_channel_t, 1> channel = {frame.channel};
    Envelope::ptr_t envelope;
    if (!ConsumeMessageOnChannelInner(channel, envelope, -1)) {
      throw std::logic_error(
          "ConsumeMessageOnChannelInner returned false unexpectedly");
    }

    m_delivered_messages.push_back(envelope);
  }
}

void Channel::ChannelImpl::GetAckOnChannel(amqp_channel_t channel) {
  
  auto& state = m_channels.at(channel);
  if (state.unconsumed_ack > 0) {
    --state.unconsumed_ack;
    return;
  }

  // If we've done things correctly we can get one of 4 things back from the
  // broker
  // - basic.ack - our channel is in confirm mode, messsage was 'dealt with' by
  // the broker
  // - basic.nack - our channel is in confirm mode, queue has max-length set and
  // is full, queue overflow stratege is reject-publish
  // - basic.return then basic.ack - the message wasn't delievered, but was
  // dealt with
  // - channel.close - probably tried to publish to a non-existant exchange, in
  // any case error!
  // - connection.close - something really bad happened
  const std::array<std::uint32_t, 3> PUBLISH_ACK = {
      AMQP_BASIC_ACK_METHOD, AMQP_BASIC_RETURN_METHOD, AMQP_BASIC_NACK_METHOD};
  amqp_frame_t response;
  std::array<amqp_channel_t, 1> channels = {channel};
  GetMethodOnChannel(channels, response, PUBLISH_ACK);

  if (AMQP_BASIC_NACK_METHOD == response.payload.method.id) {
    amqp_basic_nack_t *return_method =
        reinterpret_cast<amqp_basic_nack_t *>(response.payload.method.decoded);
    state.last_delivery_tag = return_method->delivery_tag;
    // FIXME this exception should be thrown n times in case of a multiple nack
    MessageRejectedException message_rejected(return_method->delivery_tag);
    ReturnChannel(channel);
    MaybeReleaseBuffersOnChannel(channel);
    throw message_rejected;
  }

  if (AMQP_BASIC_RETURN_METHOD == response.payload.method.id) {
    MessageReturnedException message_returned =
        CreateMessageReturnedException(
            *(reinterpret_cast<amqp_basic_return_t *>(
                response.payload.method.decoded)),
            channel);

    const std::array<std::uint32_t, 1> BASIC_ACK = {AMQP_BASIC_ACK_METHOD};
    GetMethodOnChannel(channels, response, BASIC_ACK);
    ReturnChannel(channel);
    MaybeReleaseBuffersOnChannel(channel);
    throw message_returned;
  }

  amqp_basic_ack_t *ack = reinterpret_cast<amqp_basic_ack_t *>(response.payload.method.decoded);
  if (ack->delivery_tag <= state.last_delivery_tag) {
    // FIXME should we throw an exception here?
    // throw std::runtime_error("wrong ack order");
  }
  else {
    std::uint64_t diff = ack->delivery_tag - state.last_delivery_tag;
    state.last_delivery_tag = ack->delivery_tag;
    if (diff > 1U) {
      state.unconsumed_ack = diff - 1U;
    }
  }

  ReturnChannel(channel);
  MaybeReleaseBuffersOnChannel(channel);
}

void Channel::ChannelImpl::MaybeSubscribeToDirectReply(amqp_channel_t channel) {
  auto& state = m_channels.at(channel);
  if (!state.direct_reply_tag.empty())
    return;

  const std::array<std::uint32_t, 1> CONSUME_OK = {
    AMQP_BASIC_CONSUME_OK_METHOD};
  
  std::string queue = "amq.rabbitmq.reply-to";
  std::string consumerTagIn;
  amqp_basic_consume_t consume = {};
  consume.queue = StringToBytes(queue);
  consume.consumer_tag = StringToBytes(consumerTagIn);
  consume.no_local = true;
  consume.no_ack = true;
  consume.exclusive = true;
  consume.nowait = false;

  // Detail::amqp_pool_ptr_t table_pool;
  // consume.arguments =
  //     Detail::TableValueImpl::CreateAmqpTable(arguments, table_pool);

  amqp_frame_t response = DoRpcOnChannel(
      channel, AMQP_BASIC_CONSUME_METHOD, &consume, CONSUME_OK);

  amqp_basic_consume_ok_t *consume_ok =
      (amqp_basic_consume_ok_t *)response.payload.method.decoded;
  std::string tag((char *)consume_ok->consumer_tag.bytes,
                  consume_ok->consumer_tag.len);
  MaybeReleaseBuffersOnChannel(channel);

  AddConsumer(tag, channel);
  state.direct_reply_tag = tag;
}

const std::string& Channel::ChannelImpl::GetDirectReplyToken(amqp_channel_t channel) {
  auto& state = m_channels.at(channel);
  return state.direct_reply_tag;
}

bool Channel::ChannelImpl::GetNextFrameFromBroker(
    amqp_frame_t &frame, std::chrono::microseconds timeout) {
  struct timeval *tvp = NULL;
  struct timeval tv_timeout;
  memset(&tv_timeout, 0, sizeof(tv_timeout));

  if (timeout != std::chrono::microseconds::max()) {
    // std::chrono::seconds.count() returns std::int_atleast64_t,
    // long can be 32 or 64 bit depending on the platform/arch
    // unless the timeout is something absurd cast to long will be ok, but
    // lets guard against the case where someone does something silly
    assert(std::chrono::duration_cast<std::chrono::seconds>(timeout).count() <
           static_cast<std::chrono::seconds::rep>(
               std::numeric_limits<long>::max()));

    tv_timeout.tv_sec = static_cast<long>(
        std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
    tv_timeout.tv_usec = static_cast<long>(
        (timeout - std::chrono::seconds(tv_timeout.tv_sec)).count());

    tvp = &tv_timeout;
  }

  int ret = amqp_simple_wait_frame_noblock(m_connection, &frame, tvp);

  if (AMQP_STATUS_TIMEOUT == ret) {
    return false;
  }
  CheckForError(ret);
  return true;
}

bool Channel::ChannelImpl::GetNextFrameOnChannel(
    amqp_channel_t channel, amqp_frame_t &frame,
    std::chrono::microseconds timeout) {
  frame_queue_t::iterator it = std::find_if(
      m_frame_queue.begin(), m_frame_queue.end(), [channel](auto &frame) {
        return ChannelImpl::is_on_channel(frame, channel);
      });

  if (m_frame_queue.end() != it) {
    frame = *it;
    m_frame_queue.erase(it);

    if (AMQP_FRAME_METHOD == frame.frame_type &&
        AMQP_CHANNEL_CLOSE_METHOD == frame.payload.method.id) {
      FinishCloseChannel(channel);
      AmqpException::Throw(*reinterpret_cast<amqp_channel_close_t *>(
          frame.payload.method.decoded));
    }
    return true;
  }

  std::array<amqp_channel_t, 1> channels = {channel};
  return GetNextFrameFromBrokerOnChannel(channels, frame, timeout);
}

void Channel::ChannelImpl::MaybeReleaseBuffersOnChannel(
    amqp_channel_t channel) {
  if (m_frame_queue.end() ==
      std::find_if(m_frame_queue.begin(), m_frame_queue.end(),
                   [channel](auto &frame) {
                     return Channel::ChannelImpl::is_on_channel(frame, channel);
                   })) {
    amqp_maybe_release_buffers_on_channel(m_connection, channel);
  }
}

void Channel::ChannelImpl::CheckIsConnected() {
  if (!m_is_connected) {
    throw ConnectionClosedException();
  }
}

namespace {
bool bytesEqual(amqp_bytes_t r, amqp_bytes_t l) {
  if (r.len == l.len) {
    if (0 == memcmp(r.bytes, l.bytes, r.len)) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> splitVersion(const std::string &version) {
  static char delim = '.';
  std::vector<std::string> out;
  std::size_t prev = 0;
  std::size_t cur = version.find(delim);
  while (cur != std::string::npos) {
    out.push_back(version.substr(prev, cur - prev));
    prev = cur + 1;
    cur = version.find(delim, prev);
  }
  out.push_back(version.substr(prev, cur - prev));
  return out;
}

}  // namespace

std::uint32_t Channel::ChannelImpl::ComputeBrokerVersion(
    amqp_connection_state_t state) {
  const amqp_table_t *properties = amqp_get_server_properties(state);
  const amqp_bytes_t version = amqp_cstring_bytes("version");
  amqp_table_entry_t *version_entry = NULL;

  for (int i = 0; i < properties->num_entries; ++i) {
    if (bytesEqual(properties->entries[i].key, version)) {
      version_entry = &properties->entries[i];
      break;
    }
  }
  if (NULL == version_entry) {
    return 0;
  }

  std::string version_string(
      static_cast<char *>(version_entry->value.value.bytes.bytes),
      version_entry->value.value.bytes.len);
  std::vector<std::string> version_components = splitVersion(version_string);
  if (version_components.size() != 3) {
    return 0;
  }
  std::uint32_t version_major = std::stoul(version_components[0]);
  std::uint32_t version_minor = std::stoul(version_components[1]);
  std::uint32_t version_patch = std::stoul(version_components[2]);
  return (version_major & 0xFF) << 16 | (version_minor & 0xFF) << 8 |
         (version_patch & 0xFF);
}

}  // namespace AmqpClient
