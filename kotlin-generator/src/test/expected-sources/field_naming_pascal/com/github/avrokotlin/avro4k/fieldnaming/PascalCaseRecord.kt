@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.github.avrokotlin.avro4k.fieldnaming

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlin.jvm.JvmInline
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"PascalCaseRecord","namespace":"com.github.avrokotlin.avro4k.fieldnaming","fields":[{"name":"user_id","type":"int","doc":"User identifier","default":0,"aliases":["user_identifier"]},{"name":"billing_address","type":{"type":"record","name":"BillingAddress","fields":[{"name":"street_line","type":"string","doc":"Street line","default":"unknown"}]},"doc":"Full billing address"},{"name":"account_status","type":["string",{"type":"enum","name":"StatusFlag","symbols":["ACTIVE","INACTIVE"]},"null"],"doc":"Status wrapper","aliases":["status_alias"]}]}""")
public data class PascalCaseRecord(
    /**
     * User identifier
     *
     * Default value: 0
     */
    @AvroDoc("User identifier")
    @AvroAlias("user_identifier")
    @AvroDefault("0")
    public val UserId: Int = 0,
    /**
     * Full billing address
     */
    @AvroDoc("Full billing address")
    public val BillingAddress: BillingAddress,
    /**
     * Status wrapper
     */
    @AvroDoc("Status wrapper")
    @AvroAlias("status_alias")
    public val AccountStatus: AccountStatusUnion? = null,
) {
    @Serializable
    public sealed interface AccountStatusUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : AccountStatusUnion

        @JvmInline
        @Serializable
        public value class ForStatusFlag(
            public val `value`: StatusFlag,
        ) : AccountStatusUnion
    }
}
