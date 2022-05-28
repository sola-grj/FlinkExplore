package com.dcits.toDatabase.toMysql

import com.alibaba.fastjson.JSONObject

/*
* sql 样例类
* */
case class MsgInfo(
                    sqlStr: String,
                    operation: String,
                    message: JSONObject,
                  )