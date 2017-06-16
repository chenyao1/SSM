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
package org.smartdata.server.command.executor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.smartdata.server.command.Command;
import org.smartdata.server.command.message.ActionStatusReport;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//Todo: make this a interface so that we could have different executor implementation
public class CommandExecutor {
  private Map<Long, Future> runningCommands;
  private ListeningExecutorService executorService;

  public CommandExecutor() {
    this.runningCommands = new ConcurrentHashMap<>();
    this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
  }

  public void execute(Command command) {
    ListenableFuture<?> future = this.executorService.submit(command);
    this.runningCommands.put(command.getId(), future);
    Futures.addCallback(future, new CommandCallBack(command), executorService);
  }

  public void stop(Long commandId) {
    if (this.runningCommands.containsKey(commandId)) {
      this.runningCommands.get(commandId).cancel(true);
      this.runningCommands.remove(commandId);
    }
  }

  public ActionStatusReport getActionStatusReport() {
    return new ActionStatusReport();
  }

  private class CommandCallBack implements FutureCallback<Object> {
    private final Command command;

    public CommandCallBack(Command command) {
      this.command = command;
    }

    @Override
    public void onSuccess(@Nullable Object result) {
      runningCommands.remove(this.command.getId());
    }

    @Override
    public void onFailure(Throwable t) {
      runningCommands.remove(this.command.getId());
    }
  }
}
