package com.dcits.toDatabase.toMysql

import com.alibaba.fastjson.JSONObject

import java.util
import java.util.Set

class BuildSql() {
  def build(tableName: String, pk: String, message: JSONObject, operation: String): String = {
    val data = message.getJSONObject("data")
    var sqlStr: String = ""
    val keysArray = data.keySet().toArray
    val valuesArray = data.values().toArray

    operation match {
      // INSERT
      case "INSERT" =>
        val keysStr = keysArray.mkString(",")
        val valuesStr = formatInsertValuesString(valuesArray)
        sqlStr = s"insert into ${tableName} (${keysStr}) values (${valuesStr})"
        return sqlStr
      // UPDATE
      case "UPDATE" =>
        // todo 无主键逻辑处理
        val beforeDataObj = message.getJSONObject("beforeData")
        val updateValueSql = formatUpdateValuesString(data = data, beforeData = beforeDataObj, pk = pk)
        sqlStr = s"update ${tableName} set ${updateValueSql}"
        return sqlStr
      // DELETE
      case "DELETE" =>
        // todo 无主键逻辑处理
        //        val
        return sqlStr
    }
  }

  // Insert values 拼接逻辑
  private def formatInsertValuesString(arr: Array[AnyRef]): String = {
    var resStr = ""
    for (i <- arr) {
      if (resStr == "" && i != null) {
        // 为 空字符串 拼接 ''
        resStr = s"'${i}'"
      } else if (resStr == "" && i == null) {
        resStr = "null"
      } else if (resStr != "" && i != null) {
        resStr += s",'${i}'"
      } else if (resStr != "" && i == null) {
        resStr += ",null"
      }
    }
    return resStr
  }

  /*
  * Update values 拼接逻辑
  * 有主键,beforeData 永远包含所有字段名, 只有更新字段和主键(如果有主键)有值,其他为 null, 比较beforeData和data差异值,进行更新,where 拼接主键
  * 如果没有有主键,beforeData包含全, , set 拼接 data全字段, where 拼接 before 全字段
  * */
  private def formatUpdateValuesString(data: JSONObject, beforeData: JSONObject, pk: String): String = {
    var resStr = ""
    var whereStr = ""
    val dataKeysArr = data.keySet().toArray
    val beforeDataKeysArr = data.keySet().toArray
    if (pk != "") {
      // 拼接 where
      val pkArr = pk.split(",")
      for (pkStr <- pkArr) {
        val pkValue = data.get(pkStr)
        if (whereStr == "" && pkValue != null) {
          whereStr = s" where ${pkStr} = '${pkValue}'"
        } else if (whereStr == "" && pkValue == null) {
          whereStr = s" where ${pkStr} is null "
        } else if (whereStr != "" && pkValue != null) {
          whereStr += s" and ${pkStr} = '${pkValue}' "
        } else if (whereStr != "" && pkValue == null) {
          whereStr += s" and ${pkStr} is null "
        }
      }
      // 拼接 set 值
      for (dataKey <- dataKeysArr) {
        val beforeDataValue = beforeData.get(dataKey)
        val dataValue = data.get(dataKey)
        if (beforeDataValue != dataValue) {
          if (resStr == "" && dataValue != null) {
            resStr = s" ${dataKey}='${dataValue}' "
          } else if (resStr == "" && dataValue == null) {
            resStr = s" ${dataKey}=null "
          } else if (resStr != "" && dataValue != null) {
            resStr += s" ,${dataKey}='${dataValue}' "
          } else if (resStr != "" && dataValue == null) {
            resStr += s" ,${dataKey}=null "
          }
        }
      }
    }
    else {
      // 拼接 where
      for (beforeDataKey <- beforeDataKeysArr) {
        val beforeDataValue = beforeData.get(beforeDataKey)
        if (whereStr == "" && beforeDataValue != null) {
          whereStr = s" where ${beforeDataKey} = '${beforeDataValue}'"
        } else if (whereStr == "" && beforeDataValue == null) {
          whereStr = s" where ${beforeDataKey} is null "
        } else if (whereStr != "" && beforeDataValue != null) {
          whereStr += s" and ${beforeDataKey} = '${beforeDataValue}' "
        } else if (whereStr != "" && beforeDataValue == null) {
          whereStr += s" and ${beforeDataKey} is null "
        }
      }
      // 拼接 set 值
      for (dataKey <- dataKeysArr) {
        val dataValue = data.get(dataKey)
        if (resStr == "" && dataValue != null) {
          resStr = s" ${dataKey}='${dataValue}' "
        } else if (resStr == "" && dataValue == null) {
          resStr = s" ${dataKey}=null "
        } else if (resStr != "" && dataValue != null) {
          resStr += s" ,${dataKey}='${dataValue}' "
        } else if (resStr != "" && dataValue == null) {
          resStr += s" ,${dataKey}=null "
        }
      }
    }
    resStr += whereStr
    return resStr
  }
}

