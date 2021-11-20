# seatunnel

[![Backend Workflow](https://github.com/InterestingLab/seatunnel/actions/workflows/backend.yml/badge.svg?branch=wd-v2-baseline)](https://github.com/InterestingLab/seatunnel/actions/workflows/backend.yml)
[![Join the chat at https://gitter.im/interestinglab_seatunnel/Lobby](https://badges.gitter.im/interestinglab_seatunnel/Lobby.svg)](https://gitter.im/interestinglab_seatunnel/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

---

SeaTunnel was formerly named Waterdrop , and renamed SeaTunnel since October 12, 2021.

---

SeaTunnel is a very easy-to-use ultra-high-performance distributed data integration platform that supports real-time synchronization of massive data. It can synchronize tens of billions of data stably and efficiently every day, and has been used in the production of nearly 100 companies.


## Why do we need SeaTunnel 

SeaTunnel will do its best to solve the problems that may be encountered in the synchronization of massive data:

- Data loss and duplication
- Task accumulation and delay
- Low throughput
- Long cycle to be applied in the production environment
- Lack of application running status monitoring

## SeaTunnel use scenarios

- Mass data synchronization
- Mass data integration
- ETL with massive data
- Mass data aggregation
- Multi-source data processing

## Features of SeaTunnel  

- Easy to use, flexible configuration, low code development
- Real-time streaming
- Offline multi-source data analysis
- High-performance, massive data processing capabilities
- Modular and plug-in mechanism, easy to extend
- Support data processing and aggregation by SQL
- Support Spark structured streaming
- Support Spark 2.x

## Workflow of SeaTunnel 

 [![wd-workflow.png](https://imgpp.com/images/2021/11/17/wd-workflow.png)](https://imgpp.com/image/kOKht)

Input[Data Source Input] -> Filter[Data Processing] -> Output[Result Output]  

The data processing pipeline is constituted by multiple filters to meet a variety of data processing needs. If you are accustomed to SQL, you can also directly construct a data processing pipeline by SQL, which is simple and efficient. Currently, the filter list supported by SeaTunnel is still being expanded. Furthermore, you can develop your own data processing plug-in, because the whole system is easy to expand.

## Plugins supported by SeaTunnel  

- Input plugin
Fake, File, Hdfs, Kafka, S3, Socket, self-developed Input plugin

- Filter plugin
Add, Checksum, Convert, Date, Drop, Grok, Json, Kv, Lowercase, Remove, Rename, Repartition, Replace, Sample, Split, Sql, Table, Truncate, Uppercase, Uuid, Self-developed Filter plugin

- Output plugin
Elasticsearch, File, Hdfs, Jdbc, Kafka, Mysql, S3, Stdout, self-developed Output plugin

## Environmental dependency

1. java runtime environment, java >= 8

2. If you want to run SeaTunnel in a cluster environment, any of the following Spark cluster environments is usable:

- Spark on Yarn
- Spark Standalone

If the data volume is small, or the goal is merely for functional verification, you can also start in local mode without a cluster environment, because SeaTunnel supports standalone operation. Note: SeaTunnel 2.0 supports running on Spark and Flink.

## Downloads  

Download address for run-directly software package :https://github.com/InterestingLab/SeaTunnel/releases

## Quick start

Quick start: https://interestinglab.github.io/seatunnel-docs/#/zh-cn/v1/quick-start

Detailed documentation on SeaTunnel:https://interestinglab.github.ioseatunnel-docs/#/

## Application practice cases

- Weibo, Value-added Business Department Data Platform

Weibo business uses an internal customized version of SeaTunnel and its sub-project Guardian for SeaTunnel On Yarn task monitoring for hundreds of real-time streaming computing tasks.

- Sina, Big Data Operation Analysis Platform 

Sina Data Operation Analysis Platform uses SeaTunnel to perform real-time and offline analysis of data operation and maintenance for Sina News, CDN and other services, and write it into Clickhouse.

- Sogou, Sogou Qiqian System 

Sogou Qiqian System takes SeaTunnel as an ETL tool to help establish a real-time data warehouse system.

- Qutoutiao, Qutoutiao Data Center 

Qutoutiao Data Center uses SeaTunnel to support mysql to hive offline ETL tasks, real-time hive to clickhouse backfill technical support, and well covers most offline and real-time tasks needs.

- Yixia Technology, Yizhibo Data Platform

- Yonghui Superstores Founders' Alliance-Yonghui Yunchuang Technology, Member E-commerce Data Analysis Platform 

SeaTunnel provides real-time streaming and offline SQL computing of e-commerce user behavior data for Yonghui Life, a new retail brand of Yonghui Yunchuang Technology.

- Shuidichou, Data Platform 

Shuidichou adopts SeaTunnel to do real-time streaming and regular offline batch processing on Yarn, processing 3~4T data volume average daily, and later writing the data to Clickhouse.

For more use cases, please refer to: https://interestinglab.github.io/SeaTunnel-docs/#/zh-cn/v1/case_study/

## Contribute ideas and code

Submit issues and advice: https://github.com/InterestingLab/SeaTunnel/issues

Contribute code: https://github.com/InterestingLab/SeaTunnel/pulls

## Developer

Thanks to all developers https://github.com/InterestingLab/SeaTunnel/graphs/contributors  
## Welcome to contact us

Garyelephant: garygaowork@gmail.com 
RickyHuo: huochen1994@163.com 
Chinese users can contact WeChat ID `garyelephant`, and to be invited to the WeChat user group for technical communication.


