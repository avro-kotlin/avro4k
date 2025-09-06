package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.internal.AvroGenerated
import com.squareup.kotlinpoet.Annotatable
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.MemberName
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.joinToCode
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.apache.avro.Schema
import org.apache.avro.util.internal.JacksonUtils

/**
 * Generates Kotlin classes from Avro schemas, fully compatible with avro4k.
 *
 * @param avro The Avro configuration to use mainly for logical types mapping.
 * @param unionNameFormatter A function to format the name of the generated sealed interface for union types. The default implementation appends "Union" to the provided base name.
 * @param logicalTypes A map of additional logical type names to their corresponding Kotlin class names to be treated as contextual types.
 */
public class KotlinGenerator(
    private val avro: Avro = Avro,
    private val unionNameFormatter: (String) -> String = { "${it}Union" },
    private val mapNameFormatter: (String) -> String = { "${it}Map" },
    private val arrayNameFormatter: (String) -> String = { "${it}Array" },
    logicalTypes: Map<String, String> = mapOf(),
) {
    private val logicalTypes: Map<String, SerializableTypeName> =
        avro.configuration.logicalTypes.mapValues {
            SerializableTypeName(
                typeName = ClassName.bestGuess(it.value.descriptor.serialName),
                serializableAnnotation =
                    AnnotationSpec.builder(Serializable::class)
                        .addMember("with = %T::class", it.value::class.asClassName())
                        .build()
            )
        } + logicalTypes.mapValues { parseJavaClassName(it.value) }

    /**
     * Generates Kotlin classes from the provided Avro schema.
     *
     * @param schema The Avro schema as a JSON string.
     * @param rootAnonymousSchemaName The base name to use for the root schema if it does not have a name (any schema except record, enum or fixed).
     */
    public fun generateKotlinClasses(schema: String, rootAnonymousSchemaName: String): List<FileSpec> {
        return generateRootKotlinClasses(TypeSafeSchema.from(schema), Schema.Parser().parse(schema).toString(false), rootAnonymousSchemaName.toPascalCase())
    }

    private fun TypeSpec.toFileSpec(namespace: String? = null): FileSpec {
        return FileSpec.builder(namespace?.takeIf { it.isNotEmpty() } ?: "", name!!)
            .addTypes(listOf(this))
            .build()
    }

    private fun generateRootKotlinClasses(
        schema: TypeSafeSchema,
        schemaStr: String,
        potentialAnonymousClassName: String,
    ): List<FileSpec> {
        schema.actualJavaClassName?.let {
            return listOf(generateRootValueClass(schema, schemaStr, potentialAnonymousClassName, parseJavaClassName(it).nullableIf(schema.isNullable)).toFileSpec(null))
        }
        schema.getLogicalTypeName()?.let {
            return listOf(generateRootValueClass(schema, schemaStr, potentialAnonymousClassName, it).toFileSpec(null))
        }
        return when (schema) {
            is TypeSafeSchema.NamedSchema.RecordSchema -> {
                val recordType = generateRecordClass(schema)
                listOf(recordType.toFileSpec(schema.space)) +
                    // generate nested types
                    schema.fields.flatMap { field ->
                        // the union schema is already generated as a subtype of the record, no need to generate it again. However, we need to generate the types used in the union
                        if (field.schema is TypeSafeSchema.UnionSchema) {
                            field.schema.types.flatMap { generateNestedKotlinClasses(it, potentialAnonymousClassName, emptyMap()) }
                        } else {
                            generateNestedKotlinClasses(field.schema, field.name.toPascalCase(), mapOf(schema.asClassName() to recordType))
                        }
                    }
            }

            is TypeSafeSchema.NamedSchema.EnumSchema ->
                listOf(
                    generateEnumClass(schema)
                        .addAvroGenerated(schemaStr)
                        .toFileSpec(schema.space)
                )

            is TypeSafeSchema.UnionSchema -> {
                val unionType =
                    generateUnionSealedInterface(
                        potentialAnonymousClassName,
                        potentialAnonymousClassName,
                        schema
                    )
                        .toFileSpec(null)

                listOf(unionType) + schema.types.flatMap { subType -> generateNestedKotlinClasses(subType, potentialAnonymousClassName, emptyMap()) }
            }

            is TypeSafeSchema.CollectionSchema.ArraySchema -> {
                val valueClass = generateRootValueClass(schema, schemaStr, potentialAnonymousClassName, getTypeName(schema, potentialAnonymousClassName))
                listOf(valueClass.toFileSpec(null)) +
                    (
                        schema.actualElementClass?.let { emptyList() }
                            ?: generateNestedKotlinClasses(schema.elementSchema, arrayNameFormatter(potentialAnonymousClassName), emptyMap())
                    )
            }

            is TypeSafeSchema.CollectionSchema.MapSchema -> {
                val mapType =
                    generateRootValueClass(
                        schema,
                        schemaStr,
                        potentialAnonymousClassName,
                        getTypeName(schema, potentialAnonymousClassName)
                    )
                listOf(
                    mapType.toFileSpec(null)
                ) + generateNestedKotlinClasses(schema.valueSchema, mapNameFormatter(potentialAnonymousClassName), emptyMap())
            }

            // fixed type is for now set as ByteArray, so nothing to generate
            is TypeSafeSchema.NamedSchema.FixedSchema,
            is TypeSafeSchema.PrimitiveSchema,
            ->
                listOf(
                    generateRootValueClass(
                        schema,
                        schemaStr,
                        potentialAnonymousClassName,
                        getTypeName(schema, "<primitive does not have nested type>")
                    ).toFileSpec(null)
                )
        }
    }

    private fun generateRootValueClass(schema: TypeSafeSchema, schemaStr: String, className: String, wrappedType: SerializableTypeName): TypeSpec {
        return TypeSpec.classBuilder(className)
            .addModifiers(KModifier.VALUE)
            .addAnnotation(JvmInline::class)
            .addAnnotation(Serializable::class)
            .addPrimaryProperty(
                PropertySpec.builder("value", wrappedType.typeName)
                    .addAvroDecimalAnnotation(schema)
                    .addAvroFixedAnnotation(schema)
                    .addAvroProps(schema)
                    .addImplicitAvroDefaultAnnotation(schema)
                    .apply { wrappedType.applySerializableAnnotationOn(this) }
                    .build(),
                defaultValue = getImplicitAvroPropertyDefault(schema)
            )
            .build()
            .addAvroGenerated(schemaStr)
    }

    private fun generateNestedKotlinClasses(
        schema: TypeSafeSchema,
        potentialAnonymousBaseName: String,
        generatedRecords: Map<ClassName, TypeSpec>,
    ): List<FileSpec> {
        schema.actualJavaClassName?.let {
            // nothing to generate except the root value class wrapping the already existing logical type
            return emptyList()
        }
        schema.getLogicalTypeName()?.let {
            // nothing to generate except the root value class wrapping the already existing logical type
            return emptyList()
        }
        return when (schema) {
            is TypeSafeSchema.NamedSchema.RecordSchema -> {
                val recordTypeName = schema.asClassName()
                if (recordTypeName !in generatedRecords) {
                    val recordType = generateRecordClass(schema)
                    // generate nested types
                    listOf(recordType.toFileSpec(schema.space)) +
                        schema.fields.flatMap { field ->
                            // the union schema is already generated as a subtype of the record, no need to generate it again. However, we need to generate the types used in the union
                            if (field.schema is TypeSafeSchema.UnionSchema) {
                                field.schema.types.flatMap { generateNestedKotlinClasses(it, potentialAnonymousBaseName, generatedRecords) }
                            } else {
                                generateNestedKotlinClasses(field.schema, field.name.toPascalCase(), generatedRecords + (schema.asClassName() to recordType))
                            }
                        }
                } else {
                    // recursive schema
                    emptyList()
                }
            }

            is TypeSafeSchema.NamedSchema.EnumSchema -> listOf(generateEnumClass(schema).toFileSpec(schema.space))

            is TypeSafeSchema.UnionSchema -> {
                val unionType =
                    generateUnionSealedInterface(
                        unionNameFormatter(potentialAnonymousBaseName),
                        potentialAnonymousBaseName,
                        schema
                    )
                listOf(unionType.toFileSpec(null)) +
                    schema.types.flatMap { subType -> generateNestedKotlinClasses(subType, potentialAnonymousBaseName, generatedRecords) }
            }

            is TypeSafeSchema.CollectionSchema.ArraySchema -> {
                // assuming the class already exists, nothing to generate
                (
                    schema.actualElementClass?.let { emptyList() }
                        ?: generateNestedKotlinClasses(schema.elementSchema, arrayNameFormatter(potentialAnonymousBaseName), generatedRecords)
                )
            }

            is TypeSafeSchema.CollectionSchema.MapSchema -> {
                generateNestedKotlinClasses(schema.valueSchema, mapNameFormatter(potentialAnonymousBaseName), generatedRecords)
            }

            // fixed type is for now set as ByteArray, so nothing to generate
            is TypeSafeSchema.NamedSchema.FixedSchema,
            is TypeSafeSchema.PrimitiveSchema,
            -> emptyList()
        }
    }

    private fun TypeSafeSchema.getLogicalTypeName(): SerializableTypeName? {
        return logicalTypeName?.let { this@KotlinGenerator.logicalTypes[it] }?.nullableIf(this.isNullable)
    }

    private fun getTypeName(schema: TypeSafeSchema, potentialAnonymousBaseName: String): SerializableTypeName {
        schema.actualJavaClassName?.let {
            return parseJavaClassName(it).nullableIf(schema.isNullable)
        }
        schema.getLogicalTypeName()?.let {
            return it
        }
        return when (schema) {
            is TypeSafeSchema.NamedSchema.RecordSchema,
            is TypeSafeSchema.NamedSchema.EnumSchema,
            -> {
                @Suppress("USELESS_CAST") // this is an obvious cast, but needed to convince the compiler
                (schema as TypeSafeSchema.NamedSchema).asClassName().nativelySerializable()
            }

            is TypeSafeSchema.PrimitiveSchema.StringSchema -> String::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.IntSchema -> Int::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.LongSchema -> Long::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.BooleanSchema -> Boolean::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.FloatSchema -> Float::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.DoubleSchema -> Double::class.asClassName().nativelySerializable()
            is TypeSafeSchema.PrimitiveSchema.BytesSchema,
            is TypeSafeSchema.NamedSchema.FixedSchema,
            -> ByteArray::class.asClassName().nativelySerializable()

            is TypeSafeSchema.CollectionSchema.ArraySchema -> {
                val itemType: SerializableTypeName =
                    schema.actualElementClass?.let { parseJavaClassName(it).nullableIf(schema.elementSchema.isNullable) }
                        ?: getTypeName(schema.elementSchema, arrayNameFormatter(potentialAnonymousBaseName))

                val wrapperType = List::class.asClassName().parameterizedBy(itemType.typeName)
                if (!itemType.isNativelySerializable()) {
                    // There is no way to annotate the type argument of a List, so we let the whole List be contextual
                    wrapperType.contextual()
                } else {
                    wrapperType.nativelySerializable()
                }
            }

            is TypeSafeSchema.CollectionSchema.MapSchema -> {
                val keyType =
                    schema.actualKeyClass?.let { parseJavaClassName(it) }
                        ?: String::class.asClassName().nativelySerializable()
                val valueType = getTypeName(schema.valueSchema, mapNameFormatter(potentialAnonymousBaseName))

                val wrappedType =
                    Map::class.asClassName().parameterizedBy(
                        keyType.typeName,
                        valueType.typeName
                    )
                if (!keyType.isNativelySerializable() || !valueType.isNativelySerializable()) {
                    // There is no way to annotate the type argument of a Map, so we let the whole Map be contextual
                    wrappedType.contextual()
                } else {
                    wrappedType.nativelySerializable()
                }
            }

            is TypeSafeSchema.UnionSchema -> {
                // This union will be generated, so it will be natively serializable
                ClassName("", unionNameFormatter(potentialAnonymousBaseName)).nativelySerializable()
            }
        }.nullableIf(schema.isNullable)
    }

    /**
     * Generates a sealed interface representing a complex union (more than one non-null type).
     *
     * ```kotlin
     * @Serializable
     * sealed interface <potentialAnonymousBaseName>Union {
     *     @JvmInline
     *     @Serializable
     *     value class For<Type name>(val value: <Type full name>) : <potentialAnonymousBaseName>Union
     *
     *     ...
     * }
     */
    private fun generateUnionSealedInterface(
        className: String,
        potentialAnonymousBaseName: String,
        schema: TypeSafeSchema.UnionSchema,
    ): TypeSpec {
        return TypeSpec.interfaceBuilder(className)
            .addModifiers(KModifier.SEALED)
            .addAnnotation(Serializable::class)
            .addTypes(
                run {
                    val hasSimilarNames = schema.types.groupBy { it.name }.any { it.value.size > 1 }
                    schema.types.map { subSchema ->
                        val typeName = getTypeName(subSchema, potentialAnonymousBaseName)
                        TypeSpec.classBuilder("For${(if (hasSimilarNames) subSchema.fullName else subSchema.name).toPascalCase()}")
                            .addSuperinterface(ClassName("", className))
                            .addModifiers(KModifier.VALUE)
                            .addAnnotation(JvmInline::class)
                            .addAnnotation(Serializable::class)
                            .addPrimaryProperty(
                                PropertySpec.builder("value", typeName.typeName)
                                    .addAvroDecimalAnnotation(subSchema)
                                    .addAvroFixedAnnotation(subSchema)
                                    .apply { typeName.applySerializableAnnotationOn(this) }
                                    .build()
                            )
                            .build()
                    }
                }
            )
            .build()
    }

    /**
     * Generates an enum class representing the Avro enum schema.
     *
     * ```kotlin
     * @Serializable
     * @AvroDoc("doc")
     * @AvroProp("customProp", "customValue")
     * @AvroAlias("alias1", "alias2")
     * enum class <Enum name> {
     *    A,
     *    @AvroEnumDefault
     *    B,
     *    C,
     * }
     * ```
     */
    private fun generateEnumClass(schema: TypeSafeSchema.NamedSchema.EnumSchema): TypeSpec {
        return TypeSpec.enumBuilder(schema.name)
            .addAnnotation(Serializable::class)
            .addAvroProps(schema)
            .addAvroDoc(schema)
            .addAvroAliases(schema)
            .apply {
                schema.symbols.forEach { enumSymbol ->
                    addEnumConstant(
                        enumSymbol,
                        TypeSpec.anonymousClassBuilder()
                            .apply {
                                if (enumSymbol == schema.defaultSymbol) {
                                    addAnnotation(AnnotationSpec.builder(AvroEnumDefault::class.asClassName()).build())
                                }
                            }
                            .build()
                    )
                }
            }
            .build()
    }

    private fun generateRecordClass(schema: TypeSafeSchema.NamedSchema.RecordSchema): TypeSpec {
        return (if (schema.fields.isNotEmpty()) TypeSpec.classBuilder(schema.name).addModifiers(KModifier.DATA) else TypeSpec.objectBuilder(schema.name))
            .addAnnotation(Serializable::class)
            .addAvroProps(schema)
            .addAvroDoc(schema)
            .addAvroAliases(schema)
            .let {
                schema.fields.fold(it) { builder, field ->
                    val typeName = getTypeName(field.schema, field.name.toPascalCase())

                    builder.addPrimaryProperty(
                        PropertySpec.builder(field.name, typeName.typeName)
                            .initializer(field.name)
                            .addAvroProps(field)
                            .addAvroDoc(field)
                            .addAvroAliases(field)
                            .addAvroDecimalAnnotation(field.schema)
                            .addAvroFixedAnnotation(field.schema)
                            .addAvroDefaultAnnotation(field)
                            .apply { typeName.applySerializableAnnotationOn(this) }
                            .build(),
                        defaultValue =
                            if (field.hasDefaultValue()) {
                                if (typeName.isNativelySerializable()) {
                                    // TODO contextual types needs to be converted to match well the default value
                                    // TODO recursive types needs to have a default value, or it's not possible to instantiate them
                                    getRecordFieldDefault(field.schema, field.defaultValue)
                                } else {
                                    null
                                }
                            } else {
                                getImplicitAvroPropertyDefault(field.schema)
                            }
                    )
                }
            }
            .addTypes(
                schema.fields.mapNotNull { field ->
                    if (field.schema is TypeSafeSchema.UnionSchema) {
                        generateUnionSealedInterface(unionNameFormatter(field.name.toPascalCase()), field.name.toPascalCase(), field.schema)
                    } else {
                        null
                    }
                }
            )
            .addEqualsHashCode(schema.asClassName())
            .build()
    }

    private fun getRecordFieldDefault(schema: TypeSafeSchema, fieldDefault: Any?): CodeBlock? {
        if (fieldDefault == null && schema.isNullable) return CodeBlock.of("null")

        return when (schema) {
            is TypeSafeSchema.NamedSchema.FixedSchema,
            is TypeSafeSchema.PrimitiveSchema.BytesSchema,
            -> CodeBlock.of("byteArrayOf(%L)", (fieldDefault as ByteArray).joinToString(", "))

            is TypeSafeSchema.NamedSchema.EnumSchema,
            is TypeSafeSchema.PrimitiveSchema.StringSchema,
            -> CodeBlock.of("%S", fieldDefault)

//            is TypeSafeSchema.PrimitiveSchema -> CodeBlock.of("%L", fieldDefault)
            is TypeSafeSchema.PrimitiveSchema.BooleanSchema -> CodeBlock.of("%L", fieldDefault as Boolean)
            is TypeSafeSchema.PrimitiveSchema.DoubleSchema -> CodeBlock.of("%L", fieldDefault as Double)
            is TypeSafeSchema.PrimitiveSchema.FloatSchema -> CodeBlock.of("%L", "${fieldDefault}f")
            is TypeSafeSchema.PrimitiveSchema.IntSchema -> CodeBlock.of("%L", fieldDefault as Int)
            is TypeSafeSchema.PrimitiveSchema.LongSchema -> CodeBlock.of("%L", fieldDefault as Long)

            // for union, the default has to be of the first type
            is TypeSafeSchema.UnionSchema -> getRecordFieldDefault(schema.types.first(), fieldDefault)

            is TypeSafeSchema.CollectionSchema.ArraySchema ->
                @Suppress("UNCHECKED_CAST")
                (fieldDefault as List<*>)
                    .map { getRecordFieldDefault(schema.elementSchema, it) }
                    .takeIf { it.all { it != null } }
                    ?.let {
                        getListOfCodeBlock(it as List<CodeBlock>)
                    }

            is TypeSafeSchema.CollectionSchema.MapSchema ->
                @Suppress("UNCHECKED_CAST")
                (fieldDefault as Map<*, *>)
                    .mapValues { getRecordFieldDefault(schema.valueSchema, it.value) }
                    .takeIf { it.all { it.key is String && it.value != null } }
                    ?.let { getMapOfCodeBlock(it as Map<String, CodeBlock>) }

            // TODO records' defaults are Maps, which needs to be converted to the actual record class instance
            is TypeSafeSchema.NamedSchema.RecordSchema -> null
        }
    }

    private fun PropertySpec.Builder.addImplicitAvroDefaultAnnotation(schema: TypeSafeSchema): PropertySpec.Builder {
        if (avro.configuration.implicitNulls && schema.isNullable) {
            addAnnotation(buildAvroDefaultAnnotation("null"))
        } else if (avro.configuration.implicitEmptyCollections) {
            if (schema is TypeSafeSchema.CollectionSchema.ArraySchema) {
                addAnnotation(buildAvroDefaultAnnotation("[]"))
            } else if (schema is TypeSafeSchema.CollectionSchema.MapSchema) {
                addAnnotation(buildAvroDefaultAnnotation("{}"))
            }
        }
        return this
    }

    private fun getImplicitAvroPropertyDefault(schema: TypeSafeSchema): CodeBlock? {
        if (avro.configuration.implicitNulls && schema.isNullable) {
            return CodeBlock.of("null")
        } else if (avro.configuration.implicitEmptyCollections) {
            if (schema is TypeSafeSchema.CollectionSchema.ArraySchema) {
                return getListOfCodeBlock(emptyList())
            } else if (schema is TypeSafeSchema.CollectionSchema.MapSchema) {
                return getMapOfCodeBlock(emptyMap())
            }
        }
        return null
    }

    private fun getMapOfCodeBlock(map: Map<String, CodeBlock>): CodeBlock =
        if (map.isNotEmpty()) {
            CodeBlock.of(
                "%M(%L)",
                MemberName("kotlin.collections", "mapOf"),
                map.map { (key, value) -> CodeBlock.of("%S to %L", key, value) }.joinToCode()
            )
        } else {
            CodeBlock.of("%M()", MemberName("kotlin.collections", "emptyMap"))
        }

    private fun getListOfCodeBlock(list: List<CodeBlock>): CodeBlock =
        if (list.isNotEmpty()) {
            CodeBlock.of("%M(%L)", MemberName("kotlin.collections", "listOf"), list.joinToCode())
        } else {
            CodeBlock.of("%M()", MemberName("kotlin.collections", "emptyList"))
        }
}

private data class SerializableTypeName(
    val typeName: TypeName,
    private val serializableAnnotation: AnnotationSpec?,
) {
    fun isNativelySerializable() = serializableAnnotation == null

    fun nullableIf(toBeNullable: Boolean): SerializableTypeName {
        if (toBeNullable == typeName.isNullable) return this
        return SerializableTypeName(
            typeName = typeName.copy(nullable = toBeNullable),
            serializableAnnotation = serializableAnnotation
        )
    }

    fun <T : Annotatable.Builder<T>> applySerializableAnnotationOn(builder: Annotatable.Builder<T>): Annotatable.Builder<T> {
        return if (serializableAnnotation != null) {
            builder.addAnnotation(serializableAnnotation)
        } else {
            builder
        }
    }
}

private fun TypeName.nativelySerializable() = SerializableTypeName(this, serializableAnnotation = null)

private fun TypeName.contextual() = SerializableTypeName(this, serializableAnnotation = AnnotationSpec.builder(Contextual::class.asClassName()).build())

private fun TypeSafeSchema.NamedSchema.RecordSchema.Field.unquotedDefaultValue(): String =
    JacksonUtils.toJsonNode(defaultValue)
        ?.let {
            if (it.isTextual) {
                it.asText()
            } else {
                it.toString()
            }
        } ?: "null"

private fun <T : Annotatable.Builder<B>, B : Annotatable.Builder<B>> T.addAvroProps(carrier: WithProps): T {
    carrier.props.forEach { (key, value) ->
        addAnnotation(
            AnnotationSpec.builder(AvroProp::class.asClassName())
                .addMember(CodeBlock.of("%S, %S", key, value.toString()))
                .build()
        )
    }
    return this
}

private fun buildAvroDefaultAnnotation(defaultValue: String): AnnotationSpec {
    return AnnotationSpec.builder(AvroDefault::class.asClassName())
        .addMember("%S", defaultValue)
        .build()
}

private fun <T : Annotatable.Builder<B>, B : Annotatable.Builder<B>> T.addAvroAliases(carrier: WithAliases): T {
    if (carrier.aliases.isNotEmpty()) {
        addAnnotation(
            AnnotationSpec.builder(AvroAlias::class.asClassName())
                .apply {
                    carrier.aliases.forEach { alias ->
                        addMember(CodeBlock.of("%S", alias))
                    }
                }
                .build()
        )
    }
    return this
}

private fun <T : Annotatable.Builder<B>, B : Annotatable.Builder<B>> T.addAvroDoc(carrier: WithDoc): T {
    if (carrier.doc != null) {
        addAnnotation(
            AnnotationSpec.builder(AvroDoc::class.asClassName())
                .addMember(CodeBlock.of("%S", carrier.doc))
                .build()
        )
    }
    return this
}

private fun PropertySpec.Builder.addAvroDefaultAnnotation(field: TypeSafeSchema.NamedSchema.RecordSchema.Field): PropertySpec.Builder {
    if (field.hasDefaultValue()) {
        AnnotationSpec.builder(AvroDefault::class.asClassName())
            .addMember(CodeBlock.of("%S", field.unquotedDefaultValue()))
            .build()
            .let { addAnnotation(it) }
    }
    return this
}

private fun PropertySpec.Builder.addAvroFixedAnnotation(schema: TypeSafeSchema): PropertySpec.Builder {
    if (schema is TypeSafeSchema.NamedSchema.FixedSchema) {
        AnnotationSpec.builder(AvroFixed::class.asClassName())
            .addMember(CodeBlock.of("${AvroFixed::size.name} = %L", schema.size))
            .build()
            .let { addAnnotation(it) }
    }
    return this
}

private fun PropertySpec.Builder.addAvroDecimalAnnotation(schema: TypeSafeSchema): PropertySpec.Builder {
    if (schema.logicalTypeName != "decimal") {
        return this
    }
    val scale = (schema.props["scale"] as? Int) ?: 0
    val precision = (schema.props["precision"] as? Int) ?: error("Missing 'precision' prop for 'decimal' logical type of schema $schema")
    AnnotationSpec.builder(AvroDecimal::class.asClassName())
        .addMember(CodeBlock.of("${AvroDecimal::scale.name} = %L", scale))
        .addMember(CodeBlock.of("${AvroDecimal::precision.name} = %L", precision))
        .build()
        .let { addAnnotation(it) }
    return this
}

private fun TypeSpec.addAvroGenerated(schemaStr: String): TypeSpec {
    return toBuilder()
        .addAnnotation(
            AnnotationSpec.builder(AvroGenerated::class.asClassName())
                .addMember("%P", schemaStr)
                .build()
        )
        .build()
}

/**
 * Any non word character is considered as a separator, and the next character is capitalized.
 */
private fun String.toPascalCase(): String {
    return split(Regex("\\W")).joinToString("") { it.replaceFirstChar { char -> char.uppercaseChar() } }
}

private fun TypeSafeSchema.NamedSchema.asClassName() = ClassName(space ?: "", name)

private fun parseJavaClassName(className: String): SerializableTypeName {
    return getKotlinClassReplacement(className) ?: ClassName.bestGuess(className).contextual()
}

private fun getKotlinClassReplacement(className: String): SerializableTypeName? =
    when (className) {
        String::class.java.name -> String::class.asClassName()
        Boolean::class.javaObjectType.name, Boolean::class.javaPrimitiveType!!.name -> Boolean::class.asClassName()
        Char::class.javaObjectType.name, Char::class.javaPrimitiveType!!.name -> Char::class.asClassName()
        Byte::class.javaObjectType.name, Byte::class.javaPrimitiveType!!.name -> Byte::class.asClassName()
        Short::class.javaObjectType.name, Short::class.javaPrimitiveType!!.name -> Short::class.asClassName()
        Int::class.javaObjectType.name, Int::class.javaPrimitiveType!!.name -> Int::class.asClassName()
        Long::class.javaObjectType.name, Long::class.javaPrimitiveType!!.name -> Long::class.asClassName()
        Float::class.javaObjectType.name, Float::class.javaPrimitiveType!!.name -> Float::class.asClassName()
        Double::class.javaObjectType.name, Double::class.javaPrimitiveType!!.name -> Double::class.asClassName()
        else -> null
    }?.nativelySerializable()