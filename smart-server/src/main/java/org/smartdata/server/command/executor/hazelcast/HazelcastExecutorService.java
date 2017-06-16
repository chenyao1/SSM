/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.server.command.executor.hazelcast;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.command.CommandFactory;
import org.smartdata.server.command.executor.CommandExecutorService;
import org.smartdata.server.command.executor.hazelcast.message.HazelcastMessage;
import org.smartdata.server.command.message.LaunchCommand;

import java.util.concurrent.BlockingQueue;

public class HazelcastExecutorService extends CommandExecutorService {
  public static final String COMMAND_QUEUE = "command_queue";
  public static final String SLAVE_TO_MASTER = "slave_to_master";
  private BlockingQueue<LaunchCommand> commandQueue;
  private ITopic topic;

  public HazelcastExecutorService(CommandFactory commandFactory) {
    super(commandFactory);
    this.commandQueue = HazelcastInstanceProvider.getInstance().getQueue(COMMAND_QUEUE);
    this.topic = HazelcastInstanceProvider.getInstance().getTopic(SLAVE_TO_MASTER);
    this.topic.addMessageListener(new SlaveMessageListener());
  }

  @Override
  public boolean isLocalService() {
    return false;
  }

  @Override
  public boolean canAcceptMore() {
    return HazelcastInstanceProvider.getInstance().getCluster().getMembers().size() > 1;
  }

  @Override
  public void execute(LaunchCommand command) {
    commandQueue.add(command);
  }

  private class SlaveMessageListener implements MessageListener<HazelcastMessage> {

    @Override
    public void onMessage(Message<HazelcastMessage> message) {
    }
  }
}
