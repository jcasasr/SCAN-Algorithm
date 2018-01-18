package com.tfm.miguelros.anonymization
package model


case class SimilarityPair(
                           careerLevel: Int,
                           region: Int,
                           disciplineId: Int,
                           userId1: UserID,
                           userId2: UserID,
                           score: Double,
                           debug: String=""
                         )