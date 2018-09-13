//Ejerciucio 1
def parImpar (num: Int): Boolean={if(num%2 == 0){return true}else{return false}}
println(parImpar(14))
//Ejercicio 2
val lista = List(11,1,5,3)
var x = 0

for(x <- lista){
  if(x <= x) {
    println(parImpar(x))
  }
}
//version 2.5
def verificar(lista: List[Int]): Unit = {
  for(x <- lista){
    if(x <= x) {
      println(parImpar(x))
    }
  }
}
//Ejercicio 3
val lista3 = List(11,7,5,3)
var x = 0
var sum = 0

for(x <- lista3){
  sum = sum + x
  if(x == 7) {
    sum = sum + 7
  }
}

println(sum)

//Ejercicio 4
val x = 1
def balancear(listaMid: List[Int]): Boolean={
  var primero = 0
  var segundo = 0
  segundo= listaMid.sum
  for( x <- Range(0,listaMid.length)){
    primero = primero + listaMid(x)
    segundo = segundo - listaMid(x)
    if(primero == segundo){
      return true
    }
    return false
  }
}
// version 4.2
val listaMid = List(3,1,7)
def balancear (listaMid: List[Int]): Boolean={
  val (a,b) = listaMid.splitAt(listaMid.length/2)
  if (a.sum == b.sum){ return true }
  else return false
}
println(balanceo(listaMid))


//EJercicio 5

def palindromo(palabra:String):Boolean ={
  return (palabra== palabra.reverse)
}
println(palindromo("Luis"))
println(palindromo("ana"))
