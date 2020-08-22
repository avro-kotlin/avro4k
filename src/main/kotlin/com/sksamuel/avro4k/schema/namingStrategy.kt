package com.sksamuel.avro4k.schema

interface NamingStrategy {
   fun to(name: String): String = name
}

object DefaultNamingStrategy : NamingStrategy {
   override fun to(name: String): String = name
}