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
import org.smartdata.server.cluster.HazelcastInstanceProvider;
import org.smartdata.server.command.executor.CommandExecutor;
import org.smartdata.server.command.message.LaunchCommand;

import java.util.concurrent.BlockingQueue;

public class HazelcastWorker {
  private CommandExecutor commandExecutor;
  private BlockingQueue<LaunchCommand> commandQueue;
  private ITopic topic;

  public HazelcastWorker() {
    this.commandExecutor = new CommandExecutor();
    this.commandQueue =
        HazelcastInstanceProvider.getInstance().getQueue(HazelcastExecutorService.COMMAND_QUEUE);
    this.topic =
        HazelcastInstanceProvider.getInstance().getTopic(HazelcastExecutorService.SLAVE_TO_MASTER);
  }

}
