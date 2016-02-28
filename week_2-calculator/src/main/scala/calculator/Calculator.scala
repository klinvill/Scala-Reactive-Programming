package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
      namedExpressions.map{ case (name, value) => (name, Signal(eval(value(), namedExpressions)))}
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = expr match {
    case x: Literal => x.v
    case x: Plus => eval(x.a, references) + eval(x.b, references)
    case x: Minus => eval(x.a, references) - eval(x.b, references)
    case x: Times => eval(x.a, references) * eval(x.b, references)
    case x: Divide => eval(x.a, references) / eval(x.b, references)
    case x: Ref => eval(getReferenceExpr(x.name, references), references - x.name)
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
