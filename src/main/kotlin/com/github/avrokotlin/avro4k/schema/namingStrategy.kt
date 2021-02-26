package com.github.avrokotlin.avro4k.schema

import java.util.Locale

interface NamingStrategy {
   fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name
}

object PascalCaseNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name.take(1).toUpperCase(Locale.ROOT) + name.drop(1)
}

object SnakeCaseNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name.fold(StringBuilder()) { sb, c ->
      if (c.isUpperCase())
         sb.append('_').append(c.toLowerCase())
      else
         sb.append(c.toLowerCase())
   }.toString()
}
