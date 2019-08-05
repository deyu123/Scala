package com.deyu.bigdata

case class Schema(
                   uid: String,
                   username: String,
                   gender: String,
                   level: Int,
                   is_vip: Int,
                   os: String,
                   channel: String,
                   net_config: String,
                   ip: String,
                   phone: String,
                   video_id: Int,
                   video_length: Int,
                   start_video_time: BigInt,
                   end_video_time: BigInt,
                   version: String,
                   event_key: String,
                   event_time: BigInt
                 )
