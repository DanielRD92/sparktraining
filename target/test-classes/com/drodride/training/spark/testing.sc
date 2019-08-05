import scala.util.Random

val arr = "AB".split("")
print(arr(0))

val seed = System.currentTimeMillis()
val rndGen1 = new Random(seed-63036016.toLong)
val rndGen2 = new Random(seed)
print(seed)



rndGen1.nextInt(2)
rndGen2.nextInt(2)
