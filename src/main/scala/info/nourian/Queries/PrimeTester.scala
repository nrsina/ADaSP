package info.nourian.Queries

object PrimeTester {
  def check(n: Long): Boolean = {
    if (n % 2 == 0)
      return false
    var i = 3
    while (i * i <= n) {
      if (n % i == 0)
        return false
      i += 2
    }
    true
  }

  def check(n: Int): Boolean = {
    if (n % 2 == 0)
      return false
    var i = 3
    while (i * i <= n) {
      if (n % i == 0)
        return false
      i += 2
    }
    true
  }
}
