package parsely

import java.io.File
import java.util.zip.ZipFile
import scala.collection.JavaConverters._
import scala.concurrent.{ Future, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{ Failure, Success }

object Parsely extends App {

  // Some type aliases to get more meaningful function signatures & pattern matching
  type Result = List[(String, String)]
  type ResultSet = List[(String, String, String)]

  val filePattern = "Almex.CDF.Atos.log.txt"
  val linePattern = "50 45 ([0-9]{2} ){15}?60".r

  val things = for {
    file <- new File("C:\\Data").listFiles.filter(_.getName.toLowerCase.endsWith(".zip"))
  } yield scanZip(file)

  things.foreach {
    f: Future[ResultSet] => f.onComplete {
      case Success(list: ResultSet) =>
        list.foreach(s => println(s"Watch out for my nuts ${s._1} ${s._2} ${s._3}"))
      case Failure(ex) =>
        println(s"Bork! Looks like something went wrong ${ex.getMessage}")
    }
  }

  while (!things.forall(f => f.isCompleted)) {
    Thread.sleep(100)
  }

  def scanZip(file: File): Future[ResultSet] = {
    // Create a promise: this will eventually contains a List of Strings
    val promise = Promise[ResultSet]()

    // Create the future that will fulfill the promise
    Future {
      try {
        val zip = new ZipFile(file.getAbsolutePath) // Create a ZipFile from the File object
        val entries = zip.entries.asScala // Cast the java Iterable to a Scala collection

        // Iterate through the list of entries in the zip that match the filename Regex
        val results = entries filter (_.getName.contains(filePattern)) map {
          f => {
            // Get the source from the InputStream
            val source = scala.io.Source.fromInputStream(zip.getInputStream(f))
            // Scan the source, map the result so we can add the hostname
            scanSource(source).map(s => (file.getName.take(11), s._1, s._2))
          }
        }
        // Flatten the map
        promise.success(results.flatten.toList)
      }

      // Catch errors, trigger the failure scenario for the promise
      catch {
        case nf: java.io.FileNotFoundException                  => promise.failure(nf)
        case ze: java.util.zip.ZipException                     => promise.failure(ze)
        case uc: java.nio.charset.UnmappableCharacterException  => promise.failure(uc)
        case ex: Exception                                      => promise.failure(ex)
      }
    }

    // Return the future attached to the promise
    promise.future
  }

  def scanSource(source: Source): Result = {
    // Get all lines where Regex has 1+ match
    source.getLines().filter(linePattern.findAllIn(_).nonEmpty).map {
      // If there is at least one match of the Regex, add this to the list of current matches for the .zip
      s => {
        val separated = s.split(";")
        val timeStamp = "%s %s".format(separated(0).trim, separated(1).trim)
        val dataFound = separated.last.trim
        (timeStamp, dataFound)
      }
    } toList
  }
}