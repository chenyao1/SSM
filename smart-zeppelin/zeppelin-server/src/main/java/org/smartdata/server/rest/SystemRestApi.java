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
package org.smartdata.server.rest;

import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.server.SmartEngine;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * System APIs.
 */
@Path("/system")
@Produces("application/json")
public class SystemRestApi {
  SmartEngine smartEngine;
  private static final Logger logger =
      LoggerFactory.getLogger(SystemRestApi.class);

  public SystemRestApi(SmartEngine smartEngine) {
    this.smartEngine = smartEngine;
  }

  @GET
  @Path("/version")
  public Response version() {
    return new JsonResponse<>(Response.Status.OK, "SSM version", "0.1.0").build();
  }

  @GET
  @Path("/servers")
  public void servers() {
    // return list of SmartServers and their states
  }

  @GET
  @Path("/agents")
  public void agents() {
    // return list of agents and their states
  }
}
