package bigdata.project.system

object Application extends App{
  //val pipe = new Pipe("192.168.1.3:9092","clients-purchases")
  //val platformStats = new PlatformStats("localhost:9092","clients-purchases-new-feed")
  val refereeStats = new RefereeStats("localhost:9092","clients-purchases-in")
}

