package edu.wayne.simpleisgood

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
      case o if o.startsWith("\"") => o.substring(o.indexOf("\"") + 1, o.lastIndexOf("\""))
      case o => o
    }
  }

  def preprocessUri(uri: String): String = {
    uri.substring(uri.lastIndexOf("/") + 1, uri.length).replaceAll( """\p{Punct}""", " ")
  }
}
