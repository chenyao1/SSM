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
package org.smartdata.common.protocolPB;


import com.google.protobuf.ServiceException;
import org.smartdata.common.actions.ActionDescriptor;
import org.smartdata.common.CmdletState;
import org.smartdata.common.models.ActionInfo;
import org.smartdata.common.cmdlet.CmdletDescriptor;
import org.smartdata.common.protocol.AdminServerProto.ActionDescriptorProto;
import org.smartdata.common.protocol.AdminServerProto.CmdletInfoProto;
import org.smartdata.common.protocol.AdminServerProto.RuleInfoProto;
import org.smartdata.common.protocol.ClientServerProto.ReportFileAccessEventRequestProto;
import org.smartdata.common.models.RuleInfo;
import org.smartdata.common.rule.RuleState;
import org.smartdata.common.models.CmdletInfo;
import org.smartdata.metrics.FileAccessEvent;
import org.smartdata.common.protocol.AdminServerProto.ActionInfoProto;
import org.smartdata.common.protocol.AdminServerProto.ActionInfoProto.Builder;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class PBHelper {
  private PBHelper() {
  }

  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  public static int convert(RuleState state) {
    return state.getValue();
  }

  public static RuleState convert(int state) {
    return RuleState.fromValue(state);
  }

  public static RuleInfoProto convert(RuleInfo info) {
    return RuleInfoProto.newBuilder().setId(info.getId())
        .setSubmitTime(info.getSubmitTime())
        .setLastCheckTime(info.getLastCheckTime())
        .setRuleText(info.getRuleText())
        .setNumChecked(info.getNumChecked())
        .setNumCmdsGen(info.getNumCmdsGen())
        .setRulestateProto(convert(info.getState())).build();
  }

  public static RuleInfo convert(RuleInfoProto proto) {
    return RuleInfo.newBuilder().setId(proto.getId())
        .setSubmitTime(proto.getSubmitTime())
        .setLastCheckTime(proto.getLastCheckTime())
        .setRuleText(proto.getRuleText())
        .setNumChecked(proto.getNumChecked())
        .setNumCmdsGen(proto.getNumCmdsGen())
        .setState(convert(proto.getRulestateProto())).build();
  }

  public static CmdletInfo convert(CmdletInfoProto proto) {
    // TODO replace actionType with aids
    CmdletInfo.Builder builder = CmdletInfo.newBuilder();
    builder.setCid(proto.getCid())
        .setRid(proto.getRid())
        .setState(CmdletState.fromValue(proto.getState()))
        .setParameters(proto.getParameters())
        .setGenerateTime(proto.getGenerateTime())
        .setStateChangedTime(proto.getStateChangedTime());
    List<Long> list = proto.getAidsList();
    builder.setAids(list);
    return builder.build();
  }

  public static CmdletInfoProto convert(CmdletInfo info) {
    // TODO replace actionType with aids
    CmdletInfoProto.Builder builder = CmdletInfoProto.newBuilder();
    builder.setCid(info.getCid())
        .setRid(info.getRid())
        .setState(info.getState().getValue())
        .setParameters(info.getParameters())
        .setGenerateTime(info.getGenerateTime())
        .setStateChangedTime(info.getStateChangedTime());
    builder.addAllAids(info.getAids());
    return builder.build();
  }

  public static ReportFileAccessEventRequestProto convert(FileAccessEvent event) {
    return ReportFileAccessEventRequestProto.newBuilder()
        .setFilePath(event.getPath())
        .setAccessedBy(event.getAccessedBy())
        .setFileId(event.getFileId())
        .build();
  }

  public static ActionInfoProto convert(ActionInfo actionInfo) {
    Builder builder = ActionInfoProto.newBuilder();
    builder.setActionName(actionInfo.getActionName())
        .setResult(actionInfo.getResult())
        .setLog(actionInfo.getLog())
        .setSuccessful(actionInfo.isSuccessful())
        .setCreateTime(actionInfo.getCreateTime())
        .setFinished(actionInfo.isFinished())
        .setFinishTime(actionInfo.getFinishTime())
        .setProgress(actionInfo.getProgress())
        .setActionId(actionInfo.getActionId())
        .setCmdletId(actionInfo.getCmdletId());
    builder.addAllArgs(CmdletDescriptor.toArgList(actionInfo.getArgs()));
    return builder.build();
  }

  public static ActionInfo convert(ActionInfoProto infoProto) {
    ActionInfo.Builder builder = ActionInfo.newBuilder();
    builder.setActionName(infoProto.getActionName())
        .setResult(infoProto.getResult())
        .setLog(infoProto.getLog())
        .setSuccessful(infoProto.getSuccessful())
        .setCreateTime(infoProto.getCreateTime())
        .setFinished(infoProto.getFinished())
        .setFinishTime(infoProto.getFinishTime())
        .setActionId(infoProto.getActionId())
        .setCmdletId(infoProto.getCmdletId());
    List<String> list = infoProto.getArgsList();
    try {
      builder.setArgs(CmdletDescriptor.toArgMap(list));
    } catch (ParseException e) {
      return null;
    }
    return builder.build();
  }


  public static FileAccessEvent convert(final ReportFileAccessEventRequestProto event) {
    return new FileAccessEvent(event.getFilePath(), 0, event.getAccessedBy());
  }

  public static ActionDescriptor convert(ActionDescriptorProto proto) {
    return ActionDescriptor.newBuilder()
        .setActionName(proto.getActionName())
        .setComment(proto.getComment())
        .setDisplayName(proto.getDisplayName())
        .setUsage(proto.getUsage())
        .build();
  }

  public static ActionDescriptorProto convert(ActionDescriptor ac) {
    return ActionDescriptorProto.newBuilder()
        .setActionName(ac.getActionName())
        .setComment(ac.getComment())
        .setDisplayName(ac.getDisplayName())
        .setUsage(ac.getUsage())
        .build();
  }
}
