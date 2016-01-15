package edu.wayne.simpleisgood

import java.io.StringReader

import edu.wayne.simpleisgood.belegaer.DbpediaLiteralAnalyzer
import edu.wayne.simpleisgood.belegaer.DbpediaLiteralAnalyzer
import org.apache.commons.lang.StringUtils
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

import scala.annotation.tailrec

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

  private val analyzer = new DbpediaLiteralAnalyzer(1, true)

  private def read(tokenStream: TokenStream): List[String] = read(List.empty, tokenStream)

  @tailrec
  private def read(accum: List[String], tokenStream: TokenStream): List[String] = if (!tokenStream.incrementToken) accum
  else read(accum :+ tokenStream.getAttribute(classOf[CharTermAttribute]).toString, tokenStream)

  def filterTokens(text: String): List[String] = {
    val tokenStream: TokenStream = analyzer.tokenStream("", new StringReader(text))
    tokenStream.reset
    val tokens = read(tokenStream)
    tokenStream.end
    tokenStream.close
    tokens.map(_.replace(".", ""))
  }
}
