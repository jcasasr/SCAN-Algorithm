package com.tfm.miguelros.anonymization

import com.tfm.miguelros.anonymization.model.User
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.hive.HiveContext

object Datasets {
  def getUsers(context: HiveContext): RDD[User] = {
    var sql =
      """
        select
        user_id, jobroles, career_level,
        discipline_id, industry_id, region,
        experience_years_experience,
        relations,
        interactions_title_entities,
        impressions_title_entities
        from miguelr.users_joined
      """
    if (Config.CACHE_MINI_DEMO) {
      sql = sql + " where region = 10 and discipline_id >= 1017"
      sql = sql + " and user_id % 10 = 0"
    }

    val results = context.sql(sql)
    hiveResultsToUsers(results)
  }

  private def hiveResultsToUsers(results: DataFrame): RDD[User] = {
    results.rdd.filter(! _.anyNull).map{ row =>
      val userId = row.getLong(0).toInt
      val jobRoles = row.getString(1).split(",").map(_.toInt).toSet
      val careerLevel = row.getByte(2).toInt
      val disciplineId = row.getLong(3).toInt
      val industryId = row.getLong(4).toInt
      val region = row.getInt(5)
      val yearsExperience = row.getInt(6)
      val relations = row.getString(7).split(",").filter(_.nonEmpty).map(_.toInt).toSet
      val interactionsEntities = row.getString(8).split(",").filter(_.nonEmpty).map(_.toInt).toSet
      val impressionsEntities = row.getString(9).split(",").filter(_.nonEmpty).map(_.toInt).toSet
      val entities = interactionsEntities.union(impressionsEntities)

      User(
        userId = userId,
        jobRoles = jobRoles,
        careerLevel = careerLevel,
        disciplineId = disciplineId,
        industryId = industryId,
        region = region,
        yearsExperience = yearsExperience,
        entities = entities,
        relations = relations
      )
    }
  }
}
