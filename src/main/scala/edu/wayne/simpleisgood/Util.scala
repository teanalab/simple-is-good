package edu.wayne.simpleisgood

import org.apache.commons.lang.StringUtils

/**
  * Created by fsqcds on 12/11/15.
  */
object Util {
  def cleanPart(obj: String, preprocess: Boolean): String = {
    obj match {
      case o if o.startsWith("<") => {
        val uri = o.drop(1).dropRight(1)
        if (preprocess)
          preprocessUri(uri)
        else
          uri
      }
      case o if o.startsWith("\"") => {
        val lit = o.substring(1, o.lastIndexOf("\""))
        if (preprocess)
          preprocessLiteral(lit)
        else
          lit
      }
      case o => ""
    }
  }

  def preprocessUri(uri: String): String = {
//    splitCamelCase(removePunct(uri.substring(uri.lastIndexOf("/") + 1, uri.length)))
    removePunct(uri.substring(uri.lastIndexOf("/") + 1, uri.length))
  }

  def preprocessLiteral(lit: String): String = {
//    splitCamelCase(removePunct(lit))
    removePunct(lit)
  }

//  def splitCamelCase(part: String): String = {
//    StringUtils.splitByCharacterTypeCamelCase(part).mkString(" ")
//  }

  def removePunct(part: String): String = {
    part.replaceAll( """\p{Punct}""", " ")
  }

  def extractObject(nquad: String): String = {
    nquad.substring(nquad.indexOf(' ', nquad.indexOf(' ') + 1) + 1, nquad.lastIndexOf(' ', nquad.lastIndexOf(' ') - 1))
  }
}
