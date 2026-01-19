package com.github.avrokotlin.avro4k

import com.github.avrokotlin.avro4k.SerializableTypeName.Companion.addSerializableAnnotation
import com.squareup.kotlinpoet.Annotatable
import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import kotlinx.serialization.Contextual
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.apache.avro.JsonSchemaFormatter
import org.apache.avro.ParseContext
import org.apache.avro.Schema
import java.io.File

/**
 * Generates Kotlin classes from the given list of schema files. It allows having types split across multiple files (generally done to reuse types).
 *
 * This function parses and resolves schemas for the provided files (assuming they are in json format), then generates Kotlin code for the parsed schemas.
 *
 * @param files A list of files representing the schema definitions to be parsed and converted to Kotlin code. Needs to be only files, in json avro format (no idl yet).
 * @return A list of FileSpec objects representing the generated Kotlin source code for the provided schemas.
 */
@InternalAvro4kApi
public fun KotlinGenerator.generateKotlinClassesFromFiles(files: List<File>): List<Pair<File, List<FileSpec>>> {
    // First, load all the schemas to the context to be able to resolve them
    val context = ParseContext()
    val parser = Schema.Parser(context)
    files.forEach { it to parser.parseInternal(it.readText()) }
    context.commit()
    // we cannot use the output of resolveAllSchemas, as each input file may contain multiple type, but the root anonymous name is based on the file name
    context.resolveAllSchemas()

    // Then, make a second pass to parse resolved schemas, based on the context previously loaded
    // Not perfect as it is loading twice each schema, but the schemas loaded in the context are replaced when resolved.
    return files.map { file ->
        file to generateKotlinClasses(parser.parse(file), file.nameWithoutExtension)
    }
}

/**
 * Generates Kotlin classes from Avro schemas, fully compatible with avro4k.
 *
 * @param implicitNulls nullable fields that do not have an avro default value will make generated kotlin properties as
 *                      optional with `null` default and will be decoded as null if missing from the written payload.
 *                      Defaults to true.
 * @param implicitEmptyCollections map and array fields that do not have an avro default value will make generated kotlin properties as optional
 *  *                              with `emptyList()` or `emptyMap()` default and will be decoded as an empty collection if missing from the written payload.
 *                                 Defaults to true.
 * @param unionNameFormatter A function to format the name of the generated sealed interface for union types.
 *                           The default implementation appends "Union" to the provided base name.
 * @param additionalLogicalTypes Provides a way to specify additional logical types that should be materialized with specific Kotlin classes and serializers.
 *                               All the logical types registered in the built-in [AvroConfiguration] will be present if not overridden by this parameter.
 */
@InternalAvro4kApi
public class KotlinGenerator(
    private val implicitNulls: Boolean = true,
    private val implicitEmptyCollections: Boolean = true,
    private val unionNameFormatter: (String) -> String = { "${it}Union" },
    private val mapNameFormatter: (String) -> String = { "${it}Map" },
    private val arrayNameFormatter: (String) -> String = { "${it}Array" },
    private val unionSubTypeNameFormatter: (String) -> String = { "For$it" },
    private val fieldNamingStrategy: FieldNamingStrategy = FieldNamingStrategy.Identity,
    additionalLogicalTypes: List<LogicalTypeDescriptor> = emptyList(),
) {
    @InternalAvro4kApi
    public data class LogicalTypeDescriptor(
        val logicalTypeName: String,
        val kotlinClassName: String,
        val kSerializerClassName: String? = null,
    )

    private val logicalTypes: Map<String, SerializableTypeName> = buildLogicalTypesMap(additionalLogicalTypes)

    /**
     * Generates Kotlin classes from the provided Avro schema.
     *
     * @param schema The *resolved* Avro schema. Unresolved schemas or schemas containing unresolved types will fail as it needs to know the content of the type.
     * @param rootAnonymousSchemaName The base name to use for the root schema if it does not have a name (any schema except record, enum or fixed).
     */
    public fun generateKotlinClasses(schema: Schema, rootAnonymousSchemaName: String): List<FileSpec> {
        return generateRootKotlinClasses(
            TypeSafeSchema.from(schema),
            JsonSchemaFormatter(false).format(schema),
            potentialAnonymousClassName = rootAnonymousSchemaName.toPascalCase()
        )
    }

    private fun TypeSpec.toFileSpec(namespace: String? = null): FileSpec {
        return FileSpec.builder(namespace?.takeIf { it.isNotEmpty() } ?: "", name!!)
            // @file:OptIn(InternalAvro4kApi::class, ExperimentalAvro4kApi::class)
            .addAnnotation(
                AnnotationSpec.builder(ClassName("kotlin", "OptIn"))
                    .useSiteTarget(AnnotationSpec.UseSiteTarget.FILE)
                    .addMember("%T::class", InternalAvro4kApi::class)
                    .addMember("%T::class", ExperimentalAvro4kApi::class)
                    .build()
            )
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
        schema.findLogicalTypeName()?.let {
            return listOf(generateRootValueClass(schema, schemaStr, potentialAnonymousClassName, it).toFileSpec(null))
        }
        return when (schema) {
            is TypeSafeSchema.NamedSchema.RecordSchema -> {
                val recordType =
                    generateRecordClass(schema)
                        .withAnnotation(buildAvroGeneratedAnnotation(schemaStr))
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
                        .withAnnotation(buildAvroGeneratedAnnotation(schemaStr))
                        .toFileSpec(schema.space)
                )

            is TypeSafeSchema.UnionSchema -> {
                val unionType =
                    generateSealedInterface(
                        schema,
                        potentialAnonymousClassName,
                        potentialAnonymousClassName
                    )
                        .withAnnotation(buildAvroGeneratedAnnotation(schemaStr))
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
                    .addAnnotationIfNotNull(buildAvroDecimalAnnotation(schema))
                    .addAnnotationIfNotNull(buildAvroFixedAnnotation(schema))
                    .addAnnotations(buildAvroPropAnnotations(schema))
                    .addAnnotationIfNotNull(buildImplicitAvroDefaultAnnotation(schema, implicitNulls = implicitNulls, implicitEmptyCollections = implicitEmptyCollections))
                    .addSerializableAnnotation(wrappedType)
                    .build(),
                defaultValue = buildImplicitAvroDefaultCodeBlock(schema, implicitNulls = implicitNulls, implicitEmptyCollections = implicitEmptyCollections)
            )
            .addAnnotation(buildAvroGeneratedAnnotation(schemaStr))
            .build()
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
        schema.findLogicalTypeName()?.let {
            // nothing to generate except the root value class wrapping the already existing logical type
            return emptyList()
        }
        return when (schema) {
            is TypeSafeSchema.NamedSchema.RecordSchema -> {
                val recordTypeName = schema.asClassName()
                if (recordTypeName !in generatedRecords) {
                    val recordType =
                        generateRecordClass(schema)
                            .withAnnotation(buildAvroGeneratedAnnotation(schema.originalSchema.toString()))
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

            is TypeSafeSchema.NamedSchema.EnumSchema ->
                listOf(
                    generateEnumClass(schema)
                        .withAnnotation(buildAvroGeneratedAnnotation(schema.originalSchema.toString()))
                        .toFileSpec(schema.space)
                )

            is TypeSafeSchema.UnionSchema -> {
                val unionType =
                    generateSealedInterface(
                        schema,
                        unionNameFormatter(potentialAnonymousBaseName),
                        potentialAnonymousBaseName
                    )
                        .withAnnotation(buildAvroGeneratedAnnotation(schema.originalSchema.toString()))
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

    private fun TypeSafeSchema.findLogicalTypeName(): SerializableTypeName? {
        return logicalTypeName?.let { this@KotlinGenerator.logicalTypes[it] }?.nullableIf(this.isNullable)
    }

    private fun getTypeName(schema: TypeSafeSchema, potentialAnonymousBaseName: String): SerializableTypeName {
        schema.actualJavaClassName?.let {
            return parseJavaClassName(it).nullableIf(schema.isNullable)
        }
        schema.findLogicalTypeName()?.let {
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
    private fun generateSealedInterface(
        schema: TypeSafeSchema.UnionSchema,
        className: String,
        potentialAnonymousBaseName: String,
    ): TypeSpec {
        return TypeSpec.interfaceBuilder(className)
            .addModifiers(KModifier.SEALED)
            .addAnnotation(Serializable::class)
            .addTypes(
                run {
                    val hasSimilarNames = schema.types.groupBy { it.name }.any { it.value.size > 1 }
                    schema.types.map { subSchema ->
                        val typeName = getTypeName(subSchema, potentialAnonymousBaseName)
                        TypeSpec.classBuilder(unionSubTypeNameFormatter(if (hasSimilarNames) subSchema.fullName else subSchema.name.toPascalCase()))
                            .addSuperinterface(ClassName("", className))
                            .addModifiers(KModifier.VALUE)
                            .addAnnotation(JvmInline::class)
                            .addAnnotation(Serializable::class)
                            .addPrimaryProperty(
                                PropertySpec.builder("value", typeName.typeName)
                                    .addAnnotationIfNotNull(buildAvroDecimalAnnotation(subSchema))
                                    .addAnnotationIfNotNull(buildAvroFixedAnnotation(subSchema))
                                    .addSerializableAnnotation(typeName)
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
            .addAnnotations(buildAvroPropAnnotations(schema))
            .addAnnotationIfNotNull(buildAvroDocAnnotation(schema))
            .addKDocIfNotNull(schema.doc)
            .addAnnotationIfNotNull(buildAvroAliasAnnotation(schema))
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
            .addAnnotations(buildAvroPropAnnotations(schema))
            .addAnnotationIfNotNull(buildAvroDocAnnotation(schema))
            .addKDocIfNotNull(schema.doc)
            .addAnnotationIfNotNull(buildAvroAliasAnnotation(schema))
            .let {
                schema.fields.fold(it) { builder, field ->
                    val kotlinFieldName = fieldNamingStrategy.format(field.name)
                    val typeName = getTypeName(field.schema, field.name.toPascalCase())

                    require(it.propertySpecs.none { it.name == kotlinFieldName }) {
                        "The record ${schema.fullName} contains duplicated fields when applying custom naming strategy. " +
                            "The actual avro field ${field.name} has been mapped to $kotlinFieldName which has already been added. " +
                            "Schema: $schema"
                    }

                    builder.addPrimaryProperty(
                        PropertySpec.builder(kotlinFieldName, typeName.typeName)
                            .initializer(kotlinFieldName)
                            .addAnnotations(buildAvroPropAnnotations(field))
                            .addAnnotationIfNotNull(buildAvroDocAnnotation(field))
                            .addKDocIfNotNull(field.doc)
                            .apply {
                                if (field.hasDefaultValue()) {
                                    if (kdoc.isNotEmpty()) {
                                        addKdoc("\n\n")
                                    }
                                    val defaultStr =
                                        when (val default = field.defaultValue) {
                                            is ByteArray -> default.contentToString()
                                            else -> default.toString()
                                        }
                                    addKdoc("Default value: $defaultStr")
                                }
                            }
                            .addAnnotationIfNotNull(buildAvroAliasAnnotation(field))
                            .addAnnotationIfNotNull(buildAvroDecimalAnnotation(field.schema))
                            .addAnnotationIfNotNull(buildAvroFixedAnnotation(field.schema))
                            .addAnnotationIfNotNull(buildAvroDefaultAnnotation(field))
                            .apply {
                                if (fieldNamingStrategy != FieldNamingStrategy.Identity) {
                                    addAnnotation(
                                        AnnotationSpec.builder(SerialName::class)
                                            .addMember("%S", field.name)
                                            .build()
                                    )
                                }
                            }
                            .addSerializableAnnotation(typeName)
                            .build(),
                        defaultValue =
                            if (field.hasDefaultValue()) {
                                if (typeName.isNativelySerializable()) {
                                    // TODO recursive types needs to have a default value, or it's not possible to instantiate them
                                    getRecordFieldDefault(field.schema, field.defaultValue)
                                } else {
                                    // TODO contextual types needs to be converted to match well the default value
                                    null
                                }
                            } else {
                                buildImplicitAvroDefaultCodeBlock(field.schema, implicitNulls = implicitNulls, implicitEmptyCollections = implicitEmptyCollections)
                            }
                    )
                }
            }
            .addTypes(
                schema.fields.mapNotNull { field ->
                    if (field.schema is TypeSafeSchema.UnionSchema) {
                        val unionBaseName = field.name.toPascalCase()
                        generateSealedInterface(field.schema, unionNameFormatter(unionBaseName), unionBaseName)
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

            is TypeSafeSchema.NamedSchema.EnumSchema -> CodeBlock.of("%T.%L", schema.asClassName(), fieldDefault)
            is TypeSafeSchema.PrimitiveSchema.StringSchema -> CodeBlock.of("%S", fieldDefault)
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

    companion object {
        fun <T : Annotatable.Builder<T>> T.addSerializableAnnotation(type: SerializableTypeName): T {
            return if (type.serializableAnnotation != null) {
                addAnnotation(type.serializableAnnotation)
            } else {
                this
            }
        }
    }
}

private fun TypeName.nativelySerializable() = SerializableTypeName(this, serializableAnnotation = null)

private fun TypeName.contextual() = SerializableTypeName(this, serializableAnnotation = AnnotationSpec.builder(Contextual::class.asClassName()).build())

private fun TypeName.withCustomSerializer(kSerializerType: ClassName) =
    SerializableTypeName(
        typeName = this,
        serializableAnnotation =
            AnnotationSpec.builder(Serializable::class)
                .addMember("${Serializable::with.name} = %T::class", kSerializerType)
                .build()
    )

/**
 * Any non word character is considered as a separator, and the next character is capitalized.
 */
private fun TypeSafeSchema.NamedSchema.asClassName() = ClassName(space ?: "", name)

private fun parseJavaClassName(className: String): SerializableTypeName {
    return getKotlinClassReplacement(className)?.nativelySerializable() ?: ClassName.bestGuess(className).contextual()
}

private fun buildLogicalTypesMap(logicalTypes: List<KotlinGenerator.LogicalTypeDescriptor>): Map<String, SerializableTypeName> =
    (getBuiltinLogicalTypes() + logicalTypes).associate { logicalType ->
        val serializedTypeName = parseJavaClassName(logicalType.kotlinClassName)
        if (logicalType.kSerializerClassName != null) {
            logicalType.logicalTypeName to serializedTypeName.typeName.withCustomSerializer(ClassName.bestGuess(logicalType.kSerializerClassName))
        } else {
            logicalType.logicalTypeName to serializedTypeName
        }
    }

private fun getBuiltinLogicalTypes() =
    Avro.configuration.logicalTypes.map {
        KotlinGenerator.LogicalTypeDescriptor(
            logicalTypeName = it.key,
            kotlinClassName = it.value.descriptor.serialName,
            kSerializerClassName = it.value::class.qualifiedName!!
        )
    }