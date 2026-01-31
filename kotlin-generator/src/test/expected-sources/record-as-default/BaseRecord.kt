@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.OptIn
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"BaseRecord","fields":[{"name":"emptyRecordDefault","type":{"type":"record","name":"EmptySubRecord","fields":[]},"doc":"works even for empty record, generating kotlin default","default":{}},{"name":"simpleRecordDefault","type":{"type":"record","name":"SimpleSubRecord","fields":[{"name":"intField","type":"int"},{"name":"floatField","type":"float"},{"name":"stringField","type":"string"},{"name":"bytesField","type":"bytes"},{"name":"fixedField","type":{"type":"fixed","name":"Fixed","size":3}},{"name":"enumField","type":{"type":"enum","name":"Enum","symbols":["A","B"]}},{"name":"recordField","type":{"type":"record","name":"DeepRecord","fields":[{"name":"field","type":"string"}]}}]},"default":{"intField":42,"floatField":36.0,"stringField":"abc","bytesField":"\u0001\u0002\u0003","fixedField":"\u0001\u0002\u0003","enumField":"B","recordField":{"field":"cde"}}},{"name":"recordDefaultWithCustomSerializers","type":{"type":"record","name":"SubRecordWithKSerializer","fields":[{"name":"field","type":{"type":"string","logicalType":"customLogicalTypeWithKSerializer"}}]},"doc":"as this record has custom serializer, it won't generate a kotlin property default","default":{"field":"abc"}},{"name":"recordDefaultWithContextualSerializer","type":{"type":"record","name":"SubRecordWithContextualSerializer","fields":[{"name":"field","type":{"type":"string","logicalType":"contextualLogicalType"}}]},"doc":"as this record has custom serializer, it won't generate a kotlin property default","default":{"field":"abc"}}]}""")
public data class BaseRecord(
    /**
     * works even for empty record, generating kotlin default
     *
     * Default value: {}
     */
    @AvroDoc("works even for empty record, generating kotlin default")
    @AvroDefault("{}")
    public val emptyRecordDefault: EmptySubRecord = EmptySubRecord,
    /**
     * Default value: {"intField":42,"floatField":36.0,"stringField":"abc","bytesField":"\u0001\u0002\u0003","fixedField":"\u0001\u0002\u0003","enumField":"B","recordField":{"field":"cde"}}
     */
    @AvroDefault("{\"intField\":42,\"floatField\":36.0,\"stringField\":\"abc\",\"bytesField\":\"\\u0001\\u0002\\u0003\",\"fixedField\":\"\\u0001\\u0002\\u0003\",\"enumField\":\"B\",\"recordField\":{\"field\":\"cde\"}}")
    public val simpleRecordDefault:
            SimpleSubRecord = SimpleSubRecord(intField = 42, floatField = 36.0f, stringField = "abc", bytesField = byteArrayOf(1, 2, 3), fixedField = byteArrayOf(1, 2, 3), enumField = Enum.B, recordField = DeepRecord(`field` = "cde")),
    /**
     * as this record has custom serializer, it won't generate a kotlin property default
     *
     * Default value: {"field":"abc"}
     */
    @AvroDoc("as this record has custom serializer, it won't generate a kotlin property default")
    @AvroDefault("{\"field\":\"abc\"}")
    public val recordDefaultWithCustomSerializers: SubRecordWithKSerializer,
    /**
     * as this record has custom serializer, it won't generate a kotlin property default
     *
     * Default value: {"field":"abc"}
     */
    @AvroDoc("as this record has custom serializer, it won't generate a kotlin property default")
    @AvroDefault("{\"field\":\"abc\"}")
    public val recordDefaultWithContextualSerializer: SubRecordWithContextualSerializer,
)
