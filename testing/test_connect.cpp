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

#include <gtest/gtest.h>

#include "SimpleAmqpClient/SimpleAmqpClient.h"
#include "connected_test.h"

using namespace AmqpClient;


TEST_F(connected_test, reply_to) {
  std::string queue1 = channel->DeclareQueue("");
  std::string queue2 = channel->DeclareQueue("");

  std::string consumer1 = channel->BasicConsume(queue1);
  std::string consumer2 = channel->BasicConsume(queue2);

  // 'Client side' logic. Create a request and provide reply_to queue
  BasicMessage::ptr_t outgoingRequest = BasicMessage::Create("request");
  outgoingRequest->ReplyTo(queue1);
  channel->BasicPublish("", queue2, outgoingRequest);
  
  // 'Server side' logic. Read request and send reply to reply_to queue.
  Envelope::ptr_t incomingReq = channel->BasicConsumeMessage(consumer2);
  EXPECT_EQ("request", incomingReq->Message()->Body());
  EXPECT_EQ(queue1, incomingReq->Message()->ReplyTo());
  channel->BasicPublish("", incomingReq->Message()->ReplyTo(), BasicMessage::Create("reply"));

  // 'Client side' logic. Read reply.
  Envelope::ptr_t incomingRep = channel->BasicConsumeMessage(consumer1);
  EXPECT_EQ("reply", incomingRep->Message()->Body());
}

TEST_F(connected_test, direct_reply_to) {
  std::string queue1 = channel->DeclareQueue("amq.rabbitmq.reply-to");
  std::string queue2 = channel->DeclareQueue("");

  // std::string consumer1 = channel->BasicConsume(queue1);
  std::string consumer2 = channel->BasicConsume(queue2);

  // 'Client side' logic. Create a request and provide reply_to queue
  BasicMessage::ptr_t outgoingRequest = BasicMessage::Create("request");
  outgoingRequest->ReplyTo(queue1);
  auto token = channel->BasicPublishBegin("", queue2, outgoingRequest);
  const std::string& consumer1 = channel->GetDirectReplyToken(token);
  channel->BasicPublishEnd(token);
  
  // 'Server side' logic. Read request and send reply to reply_to queue.
  Envelope::ptr_t incomingReq = channel->BasicConsumeMessage(consumer2);
  EXPECT_EQ("request", incomingReq->Message()->Body());
  // This doesn't have to be true. It normally starts with queue1 but no guarantee.
  // EXPECT_EQ(queue1, incomingReq->Message()->ReplyTo());
  channel->BasicPublish("", incomingReq->Message()->ReplyTo(), BasicMessage::Create("reply"));

  // 'Client side' logic. Read reply.
  Envelope::ptr_t incomingRep = channel->BasicConsumeMessage(consumer1);
  EXPECT_EQ("reply", incomingRep->Message()->Body());
}

TEST(connecting_test, connect_default) {
  Channel::ptr_t channel = Channel::Create(connected_test::GetBrokerHost());
}

TEST(connecting_test, connect_badhost) {
  EXPECT_THROW(Channel::ptr_t channel = Channel::Create("HostDoesntExist"),
               std::runtime_error);
}

TEST(connecting_test, open_badhost) {
  Channel::OpenOpts opts = connected_test::GetTestOpenOpts();
  opts.host = "HostDoesNotExist";
  EXPECT_THROW(Channel::ptr_t channel = Channel::Open(opts),
               std::runtime_error);
}

TEST(connecting_test, connect_badauth) {
  EXPECT_THROW(Channel::ptr_t channel = Channel::Create(
                   connected_test::GetBrokerHost(), 5672, "baduser", "badpass"),
               AccessRefusedException);
}

TEST(connecting_test, open_badauth) {
  Channel::OpenOpts opts = connected_test::GetTestOpenOpts();
  opts.auth = Channel::OpenOpts::BasicAuth("baduser", "badpass");
  EXPECT_THROW(Channel::ptr_t channel = Channel::Open(opts),
               AccessRefusedException);
}

TEST(connecting_test, connect_badframesize) {
  // AMQP Spec says we have a minimum frame size of 4096
  EXPECT_THROW(
      Channel::ptr_t channel = Channel::Create(
          connected_test::GetBrokerHost(), 5672, "guest", "guest", "/", 400),
      AmqpResponseLibraryException);
}

TEST(connecting_test, open_badframesize) {
  // AMQP Spec says we have a minimum frame size of 4096
  Channel::OpenOpts opts = connected_test::GetTestOpenOpts();
  opts.frame_max = 400;
  EXPECT_THROW(Channel::ptr_t channel = Channel::Open(opts),
               AmqpResponseLibraryException);
}

TEST(connecting_test, connect_badvhost) {
  EXPECT_THROW(Channel::ptr_t channel =
                   Channel::Create(connected_test::GetBrokerHost(), 5672,
                                   "guest", "guest", "nonexitant_vhost"),
               NotAllowedException);
}

TEST(connecting_test, open_badvhost) {
  Channel::OpenOpts opts = connected_test::GetTestOpenOpts();
  opts.vhost = "bad_vhost";
  EXPECT_THROW(Channel::ptr_t channel = Channel::Open(opts),
               NotAllowedException);
}

TEST(connecting_test, connect_using_uri) {
  std::string host_uri = "amqp://" + connected_test::GetBrokerHost();
  Channel::ptr_t channel = Channel::CreateFromUri(host_uri);
}

TEST(connecting_test, openopts_from_uri) {
  Channel::OpenOpts expected;
  expected.host = "host";
  expected.vhost = "vhost";
  expected.port = 123;
  expected.auth = Channel::OpenOpts::BasicAuth("user", "pass");

  EXPECT_EQ(expected,
            Channel::OpenOpts::FromUri("amqp://user:pass@host:123/vhost"));
}

TEST(connecting_test, openopts_from_uri_defaults) {
  Channel::OpenOpts expected;
  expected.host = "host";
  expected.vhost = "/";
  expected.port = 5672;
  expected.auth = Channel::OpenOpts::BasicAuth("guest", "guest");
  EXPECT_EQ(expected, Channel::OpenOpts::FromUri("amqp://host"));
}

TEST(connecting_test, openopts_from_amqps_uri) {
  Channel::OpenOpts expected;
  expected.host = "host";
  expected.vhost = "vhost";
  expected.port = 123;
  expected.auth = Channel::OpenOpts::BasicAuth("user", "pass");
  expected.tls_params = Channel::OpenOpts::TLSParams();
}

TEST(connecting_test, openopts_fromuri_bad) {
  EXPECT_THROW(Channel::OpenOpts::FromUri("not-a-valid-uri"), BadUriException);
}
