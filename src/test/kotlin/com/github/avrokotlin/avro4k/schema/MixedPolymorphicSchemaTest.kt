package com.github.avrokotlin.avro4k.schema

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.AvroDefault
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.apache.avro.Schema

@Serializable
sealed interface Expr
@Serializable
sealed interface UnaryExpr : Expr {
    val value : Int
}
@Serializable
sealed class BinaryExpr : Expr {
    abstract val left: Int
    abstract val right: Int
}
@Serializable
object NullaryExpr : Expr
@Serializable
data class NegateExpr(override val value : Int) : UnaryExpr
@Serializable
data class AddExpr(override val left : Int, override val right : Int) : BinaryExpr()
@Serializable
data class SubstractExpr(override val left : Int, override val right : Int) : BinaryExpr()
@Serializable
abstract class OtherBinaryExpr : BinaryExpr()
@Serializable
data class MultiplicationExpr(override val left : Int, override val right : Int) : OtherBinaryExpr()
@Serializable
abstract class OtherUnaryExpr : UnaryExpr
@Serializable
data class ConstExpr(override val value : Int) : OtherUnaryExpr()

@Serializable
data class ReferencingMixedPolymorphic(
    val notNullable: Expr
)
@Serializable
data class ReferencingNullableMixedPolymorphic(
    @AvroDefault(Avro.NULL)
    val nullable : Expr?
)
class MixedPolymorphicSchemaTest : StringSpec({
    val module = SerializersModule {        
        polymorphic(OtherUnaryExpr::class) {
            subclass(ConstExpr::class)
        }
        polymorphic(OtherBinaryExpr::class) {
            subclass(MultiplicationExpr::class)
        }
    }
    "referencing a mixed sealed hierarchy"{
        val schema = Avro(module).schema(ReferencingMixedPolymorphic.serializer())
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/mixed_polymorphic_referenced.json"))
        schema shouldBe expected
    }
    "referencing a mixed nullable sealed hierarchy"{
        val schema = Avro(module).schema(ReferencingNullableMixedPolymorphic.serializer())
        val expected = Schema.Parser().parse(javaClass.getResourceAsStream("/mixed_polymorphic_nullable_referenced.json"))
        schema shouldBe expected
    }
})