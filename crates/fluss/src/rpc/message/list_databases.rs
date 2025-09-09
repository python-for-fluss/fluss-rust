// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{impl_read_version_type, impl_write_version_type, proto};
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::message::{RequestBody, ReadVersionedType, WriteVersionedType};
use crate::rpc::frame::{ReadError, WriteError};
use crate::error::Result as FlussResult;
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct ListDatabasesRequest {
    pub inner_request: proto::ListDatabasesRequest,
}

impl ListDatabasesRequest {
    pub fn new() -> FlussResult<Self> {
        Ok(ListDatabasesRequest {
            inner_request: proto::ListDatabasesRequest {}
        })
    }
}

impl RequestBody for ListDatabasesRequest {
    type ResponseBody = proto::ListDatabasesResponse;

    const API_KEY: ApiKey = ApiKey::ListDatabases;
    const REQUEST_VERSION: ApiVersion = ApiVersion(0);
}

impl_write_version_type!(ListDatabasesRequest);
impl_read_version_type!(proto::ListDatabasesResponse);
