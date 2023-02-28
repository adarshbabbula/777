import org.apache.commons.io.FilenameUtils
def Filedf(Listfiles: List[String]): Unit = {
  for (path <- Listfiles) {
    val fileFormat = FilenameUtils.getExtension(path)
    val df = fileFormat match {
      case "csv" => spark.read.option("header", "true").csv(path)
      case "parquet" => spark.read.parquet(path)
       case "json" => spark.read.option("multiline", "true").json(path)
      case _ => throw new UnsupportedOperationException(s"File format $fileFormat is not supported")}
    df.show(false)}}
