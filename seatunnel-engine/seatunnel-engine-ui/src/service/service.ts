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

import axios, {
  AxiosRequestConfig,
  AxiosResponse,
  AxiosError,
  InternalAxiosRequestConfig
} from 'axios'
import router from '@/router'
import { useSettingStore } from '@/store/setting'
import log from '@/utils/log'

const settingStore = useSettingStore()

const handleError = (res: AxiosResponse<any, any>) => {
  if (import.meta.env.MODE === 'development') {
    log.capsule('SeaTunnel', 'UI')
    log.error(res)
  }
  window.$message.error(res.data.msg)
}

const baseRequestConfig: AxiosRequestConfig = {
  timeout: settingStore.getRequestTimeValue
    ? settingStore.getRequestTimeValue
    : 6000,
  baseURL: '/hazelcast/rest'
}

const service = axios.create(baseRequestConfig)

const err = (err: AxiosError): Promise<AxiosError> => {
  if (err.response?.status === 401) {
  }
  return Promise.reject(err)
}

service.interceptors.request.use((config: InternalAxiosRequestConfig) => {
  return config
}, err)

service.interceptors.response.use((res: AxiosResponse) => {
  switch (res.status) {
    case 200:
      return res.data

    default:
      handleError(res)
      throw new Error()
  }
}, err)

export const get = async <R>(url: string, params?: Record<string, string>) => {
  return await <R>service.get<R>(url, { params })
}
export const post = async <R>(url: string, data: Record<string, any>) => {
  return await <R>service.post<R>(url, data)
}

export { service as axios }
