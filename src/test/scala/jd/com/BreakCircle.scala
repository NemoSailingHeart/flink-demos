package jd.com

import scala.util.control.Breaks
object BreakCircle {
    def main(args: Array[String]): Unit = {
        var i = 0
//        breakable(
//            while(true){
//                if (i == 5) Breaks.break()
//                i += 1
//                println("counting " + i)
//            }
//        )

//        println("end " + i )


        while(true){
            if (i == 8) Breaks.break()
            i += 1
            println("counting " + i)
        }
    }
}
