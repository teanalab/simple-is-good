package edu.wayne.simpleisgood

import org.apache.commons.lang.StringUtils

/**
  * Created by fsqcds on 12/11/15.
  */
object Util {
  def cleanPart(obj: String, preprocess: Boolean): Option[String] = {
    obj match {
      case o if o.startsWith("<") => {
        val uri = o.drop(1).dropRight(1)
        if (preprocess)
          Some(preprocessUri(uri))
        else
          Some(uri)
      }
      case o if o.startsWith("\"") => {
        val lit = o.substring(1, o.lastIndexOf("\""))
        if (preprocess)
          Some(preprocessLiteral(lit))
        else
          Some(lit)
      }
      case o => None
    }
  }

  def preprocessUri(uri: String): String = {
    removePunct(uri.substring(uri.lastIndexOf("/") + 1, uri.length))
  }

  def preprocessLiteral(lit: String): String = {
    removePunct(lit)
  }

  def removePunct(part: String): String = {
    part.replaceAll( """\p{Punct}""", " ")
  }

  def extractObject(nquad: String): String = {
    nquad.substring(nquad.indexOf(' ', nquad.indexOf(' ') + 1) + 1, nquad.lastIndexOf(' ', nquad.lastIndexOf(' ') - 1))
  }
}
