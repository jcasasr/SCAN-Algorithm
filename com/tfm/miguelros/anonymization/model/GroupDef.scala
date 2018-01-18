package com.tfm.miguelros.anonymization
package model

case class GroupDef(careerLevel: Int, disciplineId: Int, region: Int)

object GroupDef {
  implicit val ord = Ordering.by(unapply)
}
