# <img src="src/main/graphics/logo.png" height=160>
Avro format for kotlinx.serialization 

[![Build Status](https://travis-ci.org/sksamuel/avro4k.svg?branch=master)](https://travis-ci.org/sksamuel/avro4k)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.avro4k/avro4k.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4k)
[<img src="https://img.shields.io/nexus/s/https/oss.sonatype.org/com.sksamuel.avro4k/avro4k.svg?label=latest%20snapshot&style=plastic"/>](https://oss.sonatype.org/content/repositories/snapshots/com/sksamuel/avro4k/)

## Schemas

Unlike Json, Avro is a schema based format. You'll find yourself wanting to generate schemas frequently, and writing these by hand or through the Java based `SchemaBuilder` classes can be tedious for complex domain models. Avro4s allows us to generate schemas directly from case classes at compile time via macros. This gives you both the convenience of generated code, without the annoyance of having to run a code generation step, as well as avoiding the peformance penalty of runtime reflection based code.

Let's define some classes.

```scala
case class Ingredient(name: String, sugar: Double, fat: Double)
case class Pizza(name: String, ingredients: Seq[Ingredient], vegetarian: Boolean, vegan: Boolean, calories: Int)
```

To generate an Avro Schema, we need to use the `AvroSchema` object passing in the target type as a type parameter.
This will return an `org.apache.avro.Schema` instance.

```scala
import com.sksamuel
val schema = AvroSchema[Pizza]
```

Where the generated schema is as follows:

```json
{
   "type":"record",
   "name":"Pizza",
   "namespace":"com.sksamuel",
   "fields":[
      {
         "name":"name",
         "type":"string"
      },
      {
         "name":"ingredients",
         "type":{
            "type":"array",
            "items":{
               "type":"record",
               "name":"Ingredient",
               "fields":[
                  {
                     "name":"name",
                     "type":"string"
                  },
                  {
                     "name":"sugar",
                     "type":"double"
                  },
                  {
                     "name":"fat",
                     "type":"double"
                  }
               ]
            }
         }
      },
      {
         "name":"vegetarian",
         "type":"boolean"
      },
      {
         "name":"vegan",
         "type":"boolean"
      },
      {
         "name":"calories",
         "type":"int"
      }
   ]
}
```
You can see that the schema generator handles nested case classes, sequences, primitives, etc. For a full list of supported object types, see the table later.
