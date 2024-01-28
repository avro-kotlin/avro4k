@file:UseSerializers(URLSerializer::class)

package com.github.avrokotlin.avro4k.decoder

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.serializer.URLSerializer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.net.URL

class URLDecoderTest : FunSpec({

    test("decode UT8 to URL") {

        val schema = Avro.default.schema(TestUrl.serializer())

        val record = GenericData.Record(schema)
        record.put("b", Utf8("http://www.sksamuel.com"))
        Avro.default.fromRecord(TestUrl.serializer(), record) shouldBe TestUrl(URL("http://www.sksamuel.com"))
    }
}) {
    @Serializable
    data class TestUrl(val b: URL)

    @Serializable
    data class TestUrlList(val urls: List<URL>)
}