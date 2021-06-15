package com.github.avrokotlin.avro4k.schema

interface NamingStrategy {
   fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name
}

object PascalCaseNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name.take(1).uppercase() + name.drop(1)
}

object SnakeCaseNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name.fold(StringBuilder()) { sb, c ->
      if (c.isUpperCase())
         sb.append('_').append(c.lowercase())
      else
         sb.append(c.lowercase())
   }.toString()
}
