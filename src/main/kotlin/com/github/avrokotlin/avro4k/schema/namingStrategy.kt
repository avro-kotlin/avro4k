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
    override fun to(name: String): String {
        var previousWasUppercase = true
        return name.foldIndexed(StringBuilder(name.length)) { index, sb, c ->
            if (c.isUpperCase()) {
                if (!previousWasUppercase) {
                    sb.append('_')
                }
                previousWasUppercase = true
                sb.append(c.lowercase())
            } else {
                previousWasUppercase = false
                sb.append(c)
            }
        }.toString()
    }
}
