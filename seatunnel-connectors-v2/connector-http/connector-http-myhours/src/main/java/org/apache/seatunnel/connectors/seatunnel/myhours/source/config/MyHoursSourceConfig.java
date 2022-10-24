/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.myhours.source.config;

public class MyHoursSourceConfig {
    public static final String POST = "POST";
    public static final String EMAIL = "email";
    public static final String PASSWORD = "password";
    public static final String USERS = "users";
    public static final String PROJECTS = "projects";
    public static final String ALL = "all";
    public static final String ACTIVE = "active";
    public static final String MEMBER = "member";
    public static final String CLIENT = "client";
    public static final String GRANTTYPE = "grantType";
    public static final String CLIENTID = "clientId";
    public static final String API = "api";
    public static final String AUTHORIZATION = "Authorization";
    public static final String ACCESSTOKEN = "accessToken";
    public static final String ACCESSTOKEN_PREFIX = "Bearer";
    public static final String AUTHORIZATION_URL = "https://api2.myhours.com/api/tokens/login";
    public static final String ALL_PROJECTS_URL = "https://api2.myhours.com/api/Projects/getAll";
    public static final String ACTIVE_PROJECTS_URL = "https://api2.myhours.com/api/Projects";
    public static final String ALL_MEMBERS_URL = "https://api2.myhours.com/api/Users/getAll";
    public static final String ALL_CLIENTS_URL = "https://api2.myhours.com/api/Clients";
}
